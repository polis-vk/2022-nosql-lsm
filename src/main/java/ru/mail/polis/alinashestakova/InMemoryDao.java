package ru.mail.polis.alinashestakova;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {

    private static final MemorySegment VERY_FIRST_KEY = MemorySegment.ofArray(new byte[]{});

    private ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> memory =
            new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);
    private ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> flushingMemory =
            new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);
    private final AtomicLong memorySize = new AtomicLong();

    private final AtomicBoolean isClosed = new AtomicBoolean();

    private final ExecutorService flushExecutorService = Executors.newSingleThreadExecutor();
    private final ExecutorService compactExecutorService = Executors.newSingleThreadExecutor();

    private Storage storage;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Config config;

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        this.storage = Storage.load(config);
        isClosed.set(false);
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (isClosed.get()) {
            throw new IllegalStateException("Dao is closed. You can't get entries range");
        }

        lock.readLock().lock();
        try {
            MemorySegment keyFrom = from;
            if (keyFrom == null) {
                keyFrom = VERY_FIRST_KEY;
            }

            Iterator<BaseEntry<MemorySegment>> memoryIterator = getMemoryIterator(keyFrom, to, false);
            Iterator<BaseEntry<MemorySegment>> flushingMemoryIterator = getMemoryIterator(keyFrom, to, true);
            Iterator<BaseEntry<MemorySegment>> iterator = storage.iterate(keyFrom, to);

            Iterator<BaseEntry<MemorySegment>> mergeIterator = MergeIterator.of(
                    List.of(
                            new IndexedPeekIterator<>(0, memoryIterator),
                            new IndexedPeekIterator<>(1, flushingMemoryIterator),
                            new IndexedPeekIterator<>(2, iterator)
                    ),
                    EntryKeyComparator.INSTANCE
            );

            IndexedPeekIterator<BaseEntry<MemorySegment>> delegate = new IndexedPeekIterator<>(0, mergeIterator);

            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    while (delegate.hasNext() && delegate.peek().value() == null) {
                        delegate.next();
                    }
                    return delegate.hasNext();
                }

                @Override
                public BaseEntry<MemorySegment> next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException("...");
                    }
                    return delegate.next();
                }
            };
        } finally {
            lock.readLock().unlock();
        }
    }

    private Iterator<BaseEntry<MemorySegment>> getMemoryIterator(MemorySegment from, MemorySegment to,
                                                                 boolean isFlushingMemory) {
        lock.readLock().lock();
        try {
            if (to == null) {
                return (isFlushingMemory ? flushingMemory : memory).tailMap(from).values().iterator();
            }
            return (isFlushingMemory ? flushingMemory : memory).subMap(from, to).values().iterator();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) {
        if (isClosed.get()) {
            throw new IllegalStateException("Dao is closed. You can't get entry");
        }

        lock.readLock().lock();
        try {
            Iterator<BaseEntry<MemorySegment>> iterator = get(key, null);
            if (!iterator.hasNext()) {
                return null;
            }
            BaseEntry<MemorySegment> next = iterator.next();
            if (MemorySegmentComparator.INSTANCE.compare(key, next.key()) == 0) {
                return next;
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        if (isClosed.get()) {
            throw new IllegalStateException("Dao is closed. You can't upsert entry");
        }

        lock.readLock().lock();
        try {
            long entrySize = Long.BYTES + entry.key().byteSize() + Long.BYTES
                    + (entry.value() == null ? 0 : entry.key().byteSize());

            if (memorySize.get() + entrySize > config.flushThresholdBytes()) {
                if (!flushingMemory.isEmpty()) {
                    throw new IllegalStateException("Memory is full. You can't upsert entry");
                }

                flushingMemory = memory;
                memory = createMemoryStorage();
                memory.put(entry.key(), entry);
                flushExecutorService.execute(this::autoFlush);
                return;
            }

            memory.put(entry.key(), entry);
            memorySize.addAndGet(entrySize);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void autoFlush() {
        lock.writeLock().lock();
        try {
            flushOperation(flushingMemory);
            flushingMemory = createMemoryStorage();
        } catch (IOException e) {
            throw new RuntimeException("Error during autoFlush");
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("Dao is closed. You can't flush");
        }

        lock.writeLock().lock();
        try {
            flushOperation(memory);
            memory = createMemoryStorage();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void flushOperation(ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> map)
            throws IOException {
        if (storage.isClosed() || map.isEmpty()) {
            return;
        }

        storage.close();
        Path tmp = Storage.save(config, storage, map.values().iterator());

        if (tmp != null) {
            Storage.moveFile(config, tmp, Storage.getFilesCount(config) - 1);
        }
        storage = Storage.load(config);
    }

    @Override
    public void compact() throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("Dao is closed. You can't compact");
        }

        lock.writeLock().lock();
        try {
            compactExecutorService.execute(() -> {
                try {
                    if (memory.isEmpty() && Storage.getFilesCount(config) <= 1) {
                        return;
                    }

                    Iterator<BaseEntry<MemorySegment>> allDataIterator = storage.iterate(null, null);
                    Path tmp = Storage.save(config, storage, allDataIterator);

                    if (tmp != null) {
                        Storage.deleteFiles(config);
                        Storage.moveFile(config, tmp, 0);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Error during compaction");
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (storage.isClosed() || isClosed.get()) {
            return;
        }

        isClosed.set(true);
        storage.close();
        lock.writeLock().lock();
        try {
            Path tmp = Storage.save(config, storage, memory.values().iterator());

            if (tmp != null) {
                Storage.moveFile(config, tmp, Storage.getFilesCount(config) - 1);
            }

            flushExecutorService.shutdown();
            compactExecutorService.shutdown();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static ConcurrentSkipListMap<MemorySegment, BaseEntry<MemorySegment>> createMemoryStorage() {
        return new ConcurrentSkipListMap<>(ru.mail.polis.artyomdrozdov.MemorySegmentComparator.INSTANCE);
    }
}
