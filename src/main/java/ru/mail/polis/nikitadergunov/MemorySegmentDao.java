package ru.mail.polis.nikitadergunov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final MemorySegment VERY_FIRST_KEY = MemorySegment.ofArray(new byte[]{});

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memory =
            new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);

    private final Storage storage;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Config config;

    public MemorySegmentDao(Config config) throws IOException {
        this.config = config;
        this.storage = Storage.load(config);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null) {
            from = VERY_FIRST_KEY;
        }
        Iterator<Entry<MemorySegment>> memoryIterator = getMemoryIterator(from, to);
        Iterator<Entry<MemorySegment>> iterator = storage.iterate(from, to);

        Iterator<Entry<MemorySegment>> mergeIterator = MergeIterator.of(
                List.of(
                        new IndexedPeekIterator<>(0, memoryIterator),
                        new IndexedPeekIterator<>(1, iterator)
                ),
                EntryKeyComparator.INSTANCE
        );

        IndexedPeekIterator<Entry<MemorySegment>> delegate = new IndexedPeekIterator<>(0, mergeIterator);

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                while (delegate.hasNext() && delegate.peek().value() == null) {
                    delegate.next();
                }
                return delegate.hasNext();
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("...");
                }
                return delegate.next();
            }
        };
    }

    private Iterator<Entry<MemorySegment>> getMemoryIterator(MemorySegment from, MemorySegment to) {
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
    public Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();
        if (MemorySegmentComparator.INSTANCE.compare(key, next.key()) == 0) {
            return next;
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
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
