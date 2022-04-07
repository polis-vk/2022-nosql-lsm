package ru.mail.polis.artyomdrozdov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
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

        ArrayList<Iterator<Entry<MemorySegment>>> iterators = storage.iterate(from, to);
        iterators.add(getMemoryIterator(from, to));

        Iterator<Entry<MemorySegment>> mergeIterator = MergeIterator.of(iterators, EntryKeyComparator.INSTANCE);

        return new TombstoneFilteringIterator(mergeIterator);
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
        Entry<MemorySegment> result = memory.get(key);
        if (result == null) {
            result = storage.get(key);
        }

        return (result == null || result.isTombstone()) ? null : result;
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

    private static class TombstoneFilteringIterator implements Iterator<Entry<MemorySegment>> {
        private final Iterator<Entry<MemorySegment>> iterator;
        private Entry<MemorySegment> current;

        public TombstoneFilteringIterator(Iterator<Entry<MemorySegment>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            if (current != null) {
                return true;
            }

            while (iterator.hasNext()) {
                Entry<MemorySegment> entry = iterator.next();
                if (!entry.isTombstone()) {
                    this.current = entry;
                    return true;
                }
            }

            return false;
        }

        @Override
        public Entry<MemorySegment> next() {
            if (!hasNext()) {
                throw new NoSuchElementException("...");
            }
            Entry<MemorySegment> next = current;
            current = null;
            return next;
        }
    }
}
