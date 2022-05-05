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

    // must be lower than 1
    private static final int ORDER_FIRST = -1;
    // must be lower than 1
    private static final int ORDER_SECOND = 0;

    private final ExecutorService executor = Executors.newSingleThreadExecutor(
            (r) -> new Thread(r, "MemoryAndDiskDao")
    );

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final List<MappedByteBuffer> files = new ArrayList<>();

    private final List<MappedByteBuffer> fileIndexes = new ArrayList<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);

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
            list.add(new PeekIterator<>(temp.iterator(), ORDER_FIRST));
        }
        if (onFlushCollection != null) {
            temp = FileUtils.getInMemoryCollection(onFlushCollection, from, to);
            if (!temp.isEmpty()) {
                list.add(new PeekIterator<>(temp.iterator(), ORDER_SECOND));
            }
        }
        list.addAll(getFilesCollection(files, fileIndexes, from, to));
        return new MergingIterator(list);
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        validate();

        boolean needFlush = false;
        lock.readLock().lock();
        try {
            BaseEntry<ByteBuffer> previous = collection.put(entry.key(), entry);
            int addBytes = Integer.BYTES + FileUtils.sizeOfEntry(entry)
                    - ((previous == null) ? 0 : FileUtils.sizeOfEntry(previous));
            if (memBytes.addAndGet(addBytes) > memMaxBytes) {
                needFlush = true;
            }
        } finally {
            lock.readLock().unlock();
        }
        if (needFlush) {
            autoFlush();
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        validate();

        if (collection.isEmpty()) {
            return;
        }
        if (onFlushCollection != null) {
            throw new IllegalStateException("Can't flush more");
        }

        executeFlush();
    }

    @Override
    public void compact() throws IOException {
        validate();
        if (!(filesCount.get() > 1 || (filesCount.get() == 1 && !FileUtils.isCompacted(path, 1)))) {
            //don't need compaction
            return;
        }
        executor.submit(this::executeCompact);
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed.getAndSet(true)) {
            return;
        }
        executor.shutdown();
        try {
            // waits infinitely
            while (!executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS)) ;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        executeFlush();
    }

    private void createMemoryData() {
        collection = new ConcurrentSkipListMap<>();
    }

    private void validate() {
        if (closed.get()) {
            throw new UnsupportedOperationException("DAO is closed");
        }
    }

    private synchronized void autoFlush() {
        if (onFlushCollection != null) {
            throw new IllegalStateException("Can't autoflush more");
        }
        executor.submit(this::executeFlush);
    }

    private void executeFlush() {
        if (collection.isEmpty()) {
            return;
        }
        try {
            lock.writeLock().lock();
            try {
                if (collection.isEmpty()) {
                    return;
                }
                if (onFlushCollection != null) {
                    return; // already flushing
                }
                onFlushCollection = collection;
                createMemoryData();
                memBytes.set(0);
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

    private void executeCompact() {
        int count = filesCount.get();
        try {
            FileUtils.compact(new MergingIterator(getFilesCollection(files, fileIndexes, null, null)), path);
            FileUtils.clearOldFiles(count, path);
        filesCount.set(1);
        } catch (IOException e) {
            LOG.error("Error while flushing", e);
            throw new UncheckedIOException(e);
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
