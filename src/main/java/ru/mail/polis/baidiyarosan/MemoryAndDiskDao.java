package ru.mail.polis.baidiyarosan;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    private static final int TIMEOUT_MIN = 5;

    private final ExecutorService exec = Executors.newFixedThreadPool(2);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final List<MappedByteBuffer> files = new ArrayList<>();

    private final List<MappedByteBuffer> fileIndexes = new ArrayList<>();

    private final Path path;

    private final long memMaxBytes;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final AtomicLong memBytes = new AtomicLong(0);

    private final AtomicInteger filesCount = new AtomicInteger(0);

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
        FileUtils.clearOldFiles(filesCount.get(), path, fileIndexes, files);
    }

    private void createMemoryData() {
        collection = new ConcurrentSkipListMap<>();
    }

    private void freeMemoryData() {
        if (!collection.isEmpty()) {
            createMemoryData();
        }
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
        list.addAll(FileUtils.getFilesCollection(filesCount.get(), path, files, fileIndexes, from, to));
        return new MergingIterator(list);
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        validate();
        lock.readLock().lock();
        try {
            int entrySize = FileUtils.sizeOfEntry(entry);
            if (memBytes.addAndGet(entrySize) > memMaxBytes) {
                try {
                    flush();
                } catch (IOException e) {
                    // can't access file system
                    throw new UncheckedIOException(e);
                }
                memBytes.addAndGet(entrySize);
            }
            collection.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        validate();

        if (collection.isEmpty()) {
            return;
        }
        if (onFlushCollection != null) {
            throw new UnsupportedOperationException("Can't flush more");
        }

        exec.execute(() -> {
            lock.writeLock().lock();
            try {
                onFlushCollection = collection;
                freeMemoryData();
                memBytes.set(0);
                FileUtils.writeOnDisk(onFlushCollection, path);
                onFlushCollection = null;
                filesCount.incrementAndGet();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                lock.writeLock().unlock();
            }
        });
    }

    @Override
    public void compact() throws IOException {
        validate();
        if (!(filesCount.get() > 1 || (filesCount.get() == 1 && !FileUtils.isCompacted(path, 1)))) {
            //don't need compaction
            return;
        }
        exec.execute(() -> {
            lock.writeLock().lock();
            try {
                int count = filesCount.get();
                FileUtils.compact(new MergingIterator(FileUtils.getFilesCollection(count, path, files, fileIndexes)), path);
                FileUtils.clearOldFiles(count, path, fileIndexes, files);
                filesCount.set(1);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                lock.writeLock().unlock();
            }
        });
    }

    @Override
    public void close() throws IOException {
        if (isClosed.get()) {
            return;
        }
        flush();
        exec.shutdown();
        try {
            // wait until done some time then terminate 'em all
            if (!exec.awaitTermination(TIMEOUT_MIN, TimeUnit.MINUTES)) {
                exec.shutdownNow();
                throw new RuntimeException("Close timeout for " + TIMEOUT_MIN + " min");
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        isClosed.set(true);
        FileUtils.clearAllFrom(files);
        FileUtils.clearAllFrom(fileIndexes);

    }

    private void validate() {
        if (isClosed.get()) {
            throw new UnsupportedOperationException("DAO is closed");
        }
    }

}
