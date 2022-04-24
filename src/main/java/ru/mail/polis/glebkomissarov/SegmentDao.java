package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class SegmentDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private static final MemorySegment FIRST_KEY = MemorySegment.ofArray(new byte[]{});
    private static final Logger log = Logger.getLogger(SegmentDao.class.getName());

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Path basePath;
    private final Storage storage;

    private ConcurrentSkipListMap<MemorySegment, BaseEntry<MemorySegment>> inMemory = getNewMap();
    private Collection<BaseEntry<MemorySegment>> entriesToFlush;

    // Threads for flush() & compact()
    private final ExecutorService flusher = Executors.newSingleThreadExecutor();
    private final ExecutorService compacter = Executors.newSingleThreadExecutor();

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicBoolean isAutoFlushed = new AtomicBoolean(false);
    private final AtomicLong bytesInMemory = new AtomicLong(0);

    // Maximum 1 flush executing + 1 in queue
    private final AtomicInteger count = new AtomicInteger(0);

    private final long bytesToFlush;

    public SegmentDao(Config config) throws IOException {
        basePath = config.basePath();
        bytesToFlush = config.flushThresholdBytes();
        storage = Storage.load(basePath);
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) throws IOException {
        try {
            lock.readLock().lock();
            return new FilterTombstonesIterator(new MergeIterator(listOfIterators(from, to)));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) throws IOException {
        try {
            lock.readLock().lock();
            BaseEntry<MemorySegment> entry = inMemory.get(key);
            if (entry == null) {
                return storage.get(key);
            }
            return entry.value() == null ? null : entry;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        lock.writeLock().lock();
        if (!checkAvailability()) {
            throw new RuntimeException("Too much data is being written to disk");
        }

        try {
            inMemory.put(entry.key(), entry);
            long valueSize = entry.value() == null ? Long.BYTES : entry.value().byteSize();
            long size = entry.key().byteSize() + valueSize;
            if (bytesInMemory.addAndGet(size) >= bytesToFlush) {
                // save entries to flush
                entriesToFlush = inMemory.values();
                // Update map
                inMemory = getNewMap();
                bytesInMemory.set(0);
                isAutoFlushed.set(true);

                flush();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void compact() {
        compacter.execute(() -> {
            Iterator<BaseEntry<MemorySegment>> all;
            synchronized (this) {
                // Synchronized with SSTables update from flush()
                // because allIterators() uses filled SSTable to get merge iterator
                MergeIterator merged = new MergeIterator(storage.allIterators(null, null));
                all = new FilterTombstonesIterator(merged);
            }

            try {
                boolean isCompacted = storage.compact(all, basePath);
                if (isCompacted) {
                    try {
                        // Can't use get/update
                        lock.writeLock().lock();
                        storage.update(basePath);
                    } finally {
                        lock.writeLock().unlock();
                    }
                }
            } catch (IOException e) {
                log.severe("Broken files");
            }
        });
    }

    @Override
    public void flush() {
        if (count.get() > 1) {
            throw new RuntimeException("Too much data is being written to disk");
        }

        entriesToFlush = entriesToFlush == null ? inMemory.values() : entriesToFlush;
        count.incrementAndGet();

        flusher.execute(() -> {
            try {
                storage.save(entriesToFlush, basePath);
                entriesToFlush = null;
                // Synchronized with allIterators() from compact()
                synchronized (this) {
                    // Lock to update files from disk & in memory map
                    lock.writeLock().lock();
                    try {
                        if (isAutoFlushed.compareAndSet(true, false)) {
                            inMemory = getNewMap();
                        }
                        storage.update(basePath);
                    } finally {
                        count.decrementAndGet();
                        lock.writeLock().unlock();
                    }
                }
            } catch (IOException e) {
                log.severe("Broken Files");
            }
        });
    }

    @Override
    public void close() throws IOException {
        // Only one close()
        if (isClosed.compareAndSet(false, true)) {
            try {
                if (!inMemory.isEmpty()) {
                    flush();
                }

                compacter.shutdown();
                flusher.shutdown();
                try {
                    boolean isCompacted = compacter.awaitTermination(1, TimeUnit.SECONDS);
                    boolean isFlushed = flusher.awaitTermination(1, TimeUnit.SECONDS);
                    if (!isCompacted || !isFlushed) {
                        throw new RuntimeException("Timeout");
                    }
                } catch (InterruptedException e) {
                    log.severe("Unexpected interrupt");
                }
            } finally {
                storage.close();
            }
        }
    }

    private List<PeekIterator> listOfIterators(MemorySegment from, MemorySegment to) {
        MemorySegment newFrom = from;
        if (from == null) {
            newFrom = FIRST_KEY;
        }

        Iterator<BaseEntry<MemorySegment>> memoryIterator;
        if (to == null) {
            memoryIterator = inMemory.tailMap(newFrom).values().iterator();
        } else {
            memoryIterator = inMemory.subMap(newFrom, to).values().iterator();
        }

        List<PeekIterator> iterators = storage.allIterators(from, to);
        iterators.add(new PeekIterator(memoryIterator, 0));
        return iterators;
    }

    private ConcurrentSkipListMap<MemorySegment, BaseEntry<MemorySegment>> getNewMap() {
        return new ConcurrentSkipListMap<>(Comparator::compare);
    }

    private boolean checkAvailability() {
        return bytesInMemory.get() < bytesToFlush;
    }
}
