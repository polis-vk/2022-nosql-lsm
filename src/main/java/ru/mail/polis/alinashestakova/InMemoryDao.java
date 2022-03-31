package ru.mail.polis.alinashestakova;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {

    private static final MemorySegment VERY_FIRST_KEY = MemorySegment.ofArray(new byte[]{});

    private final ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> memory =
            new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);

    private final Storage storage;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Config config;

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        this.storage = Storage.load(config);
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        MemorySegment keyFrom = from;
        if (keyFrom == null) {
            keyFrom = VERY_FIRST_KEY;
        }

        Iterator<BaseEntry<MemorySegment>> memoryIterator = getMemoryIterator(keyFrom, to);
        Iterator<BaseEntry<MemorySegment>> iterator = storage.iterate(keyFrom, to);

        Iterator<BaseEntry<MemorySegment>> mergeIterator = MergeIterator.of(
                List.of(
                        new IndexedPeekIterator<>(0, memoryIterator),
                        new IndexedPeekIterator<>(1, iterator)
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
    }

    private Iterator<BaseEntry<MemorySegment>> getMemoryIterator(MemorySegment from, MemorySegment to) {
        lock.readLock().lock();
        try {

            if (to == null) {
                return memory.tailMap(from).values().iterator();
            }

            return memory.subMap(from, to).values().iterator();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) {
        Iterator<BaseEntry<MemorySegment>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }
        BaseEntry<MemorySegment> next = iterator.next();
        if (MemorySegmentComparator.INSTANCE.compare(key, next.key()) == 0) {
            return next;
        }
        return null;
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        lock.readLock().lock();
        try {
            memory.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void close() throws IOException {
        if (storage.isClosed()) {
            return;
        }

        storage.close();
        lock.writeLock().lock();
        try {
            Storage.save(config, storage, memory.values());
        } finally {
            lock.writeLock().unlock();
        }
    }
}
