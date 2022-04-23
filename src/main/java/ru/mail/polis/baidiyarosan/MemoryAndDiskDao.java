package ru.mail.polis.baidiyarosan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemoryAndDiskDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryAndDiskDao.class);

    private final ExecutorService executor = Executors.newSingleThreadExecutor(
            (r) -> new Thread(r, "MemoryAndDiskDao")
    );

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final List<MappedByteBuffer> files = new ArrayList<>();

    private final List<MappedByteBuffer> fileIndexes = new ArrayList<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final AtomicBoolean oversized = new AtomicBoolean(false);

    private final AtomicLong memBytes = new AtomicLong(0);

    private final AtomicInteger filesCount = new AtomicInteger(0);

    private final Path path;

    private final long memMaxBytes;

    private volatile NavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> collection;
    // flush queue is single file
    private volatile NavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> onFlushCollection;

    public MemoryAndDiskDao(Config config) throws IOException {
        this.path = config.basePath();
        this.memMaxBytes = config.flushThresholdBytes();
        createMemoryData();
        this.filesCount.set(FileUtils.getPaths(path).size());
        Path indexesDir = path.resolve(Paths.get(FileUtils.INDEX_FOLDER));
        if (Files.notExists(indexesDir)) {
            Files.createDirectory(indexesDir);
        }
        FileUtils.clearOldFiles(filesCount.get(), path);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        validate();

        List<PeekIterator<BaseEntry<ByteBuffer>>> list = new LinkedList<>();
        Collection<BaseEntry<ByteBuffer>> temp = FileUtils.getInMemoryCollection(collection, from, to);
        if (!temp.isEmpty()) {
            list.add(new PeekIterator<>(temp.iterator(), 0));
        }
        if (onFlushCollection != null) {
            temp = FileUtils.getInMemoryCollection(collection, from, to);
            if (!temp.isEmpty()) {
                list.add(new PeekIterator<>(temp.iterator(), 0));
            }
        }
        list.addAll(getFilesCollection(files, fileIndexes, from, to));
        return new MergingIterator(list);
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        validate();
        lock.readLock().lock();
        boolean needFlush = false;
        try {
            BaseEntry<ByteBuffer> previous = collection.put(entry.key(), entry);
            int addBytes = FileUtils.sizeOnDisk(entry) - ((previous != null) ? FileUtils.sizeOnDisk(previous) : 0);
            if (memBytes.addAndGet(addBytes) > memMaxBytes) {
                needFlush = oversized.getAndSet(true);
            }
        } finally {
            lock.readLock().unlock();
        }

        if (needFlush) {
            autoFlush();
        }
    }

    private void createMemoryData() {
        collection = new ConcurrentSkipListMap<>();
    }

    private synchronized void autoFlush() {
        if (collection.isEmpty()) {
            return;
        }
        if (onFlushCollection != null) {
            throw new UnsupportedOperationException("Can't flush more");
        }
        executor.submit(this::executeFlush);
    }

    @Override
    public synchronized void flush() throws IOException {
        validate();

        if (collection.isEmpty()) {
            return;
        }
        if (onFlushCollection != null) {
            throw new UnsupportedOperationException("Can't flush more");
        }

        //executor.execute(this::executeFlush);
        executeFlush();
    }

    private void executeFlush() {
        if (collection.isEmpty()) {
            return;
        }

        try {
            lock.writeLock().lock();
            try {
                onFlushCollection = collection;
                createMemoryData();
                memBytes.set(0);
                oversized.set(false);
            } finally {
                lock.writeLock().unlock();
            }

            FileUtils.flush(onFlushCollection, path);
            lock.writeLock().lock();

            try {
                onFlushCollection = null;
                filesCount.incrementAndGet();
            } finally {
                lock.writeLock().unlock();
            }

        } catch (Exception e) {
            LOG.error("Error while flushing", e);
            // don't know how to do better
            System.exit(-1);
        }
    }

    @Override
    public synchronized void compact() throws IOException {
        validate();
        if (!(filesCount.get() > 1 || (filesCount.get() == 1 && !FileUtils.isCompacted(path, 1)))) {
            //don't need compaction
            return;
        }
        executeCompact();
    }

    private void executeCompact() {
        lock.writeLock().lock();
        try {
            int count = filesCount.get();
            FileUtils.compact(new MergingIterator(getFilesCollection(files, fileIndexes, null, null)), path);
            FileUtils.clearOldFiles(count, path);
            filesCount.set(1);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed.get()) {
            return;
        }
        executeFlush();
        executor.shutdown();
        try {
            // waits infinitely
            while (!executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS)) ;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        closed.set(true);
    }


    private void validate() {
        if (closed.get()) {
            throw new UnsupportedOperationException("DAO is closed");
        }
    }

    private Collection<PeekIterator<BaseEntry<ByteBuffer>>>
    getFilesCollection(List<MappedByteBuffer> files, List<MappedByteBuffer> fileIndexes,
                       ByteBuffer from, ByteBuffer to) throws IOException {
        List<PeekIterator<BaseEntry<ByteBuffer>>> list = new LinkedList<>();
        int count = this.filesCount.get();
        Collection<BaseEntry<ByteBuffer>> temp;
        for (int i = 0; i < count; ++i) {
            // file naming starts from 1, collections ordering starts from 0
            Path filePath;
            if (FileUtils.isCompacted(path, i + 1)) {
                filePath = FileUtils.getCompactedDataPath(path, i + 1);
            } else {
                filePath = FileUtils.getDataPath(path, i + 1);
            }
            Path indexPath = FileUtils.getIndexPath(path, i + 1);
            if (files.size() <= i || files.get(i) == null) {
                try (FileChannel in = FileChannel.open(filePath, StandardOpenOption.READ);
                     FileChannel indexes = FileChannel.open(indexPath, StandardOpenOption.READ)
                ) {
                    files.add(i, in.map(FileChannel.MapMode.READ_ONLY, 0, in.size()));
                    fileIndexes.add(i, indexes.map(FileChannel.MapMode.READ_ONLY, 0, indexes.size()));
                }
            }

            temp = FileUtils.getInFileCollection(files.get(i), fileIndexes.get(i), from, to);
            if (!temp.isEmpty()) {
                list.add(new PeekIterator<>(temp.iterator(), count - i));
            }
        }

        return list;
    }

}
