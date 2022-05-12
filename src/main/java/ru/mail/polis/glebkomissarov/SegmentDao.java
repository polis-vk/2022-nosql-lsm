package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SegmentDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private static final MemorySegment FIRST_KEY = MemorySegment.ofArray(new byte[]{});

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Path basePath;

    private ConcurrentSkipListMap<MemorySegment, BaseEntry<MemorySegment>> inMemory = new ConcurrentSkipListMap<>(
            Comparator::compare
    );
    private Storage storage;

    public SegmentDao(Config config) throws IOException {
        basePath = config.basePath();
        storage = Storage.load(basePath);
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) throws IOException {
        return new FilterTombstonesIterator(new MergeIterator(listOfIterators(from, to)));
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) throws IOException {
        BaseEntry<MemorySegment> entry = inMemory.get(key);
        if (entry == null) {
            return storage.get(key);
        }
        return entry.value() == null ? null : entry;
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        lock.writeLock().lock();
        try {
            inMemory.put(entry.key(), entry);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        boolean isEnded = storage.compact(all(), basePath, inMemory.isEmpty());
        if (isEnded) {
            inMemory = getNewMap();
        }
    }

    @Override
    public void flush() throws IOException {
        storage.save(inMemory.values(), basePath);
        inMemory = getNewMap();
        storage = Storage.load(basePath);
    }

    @Override
    public void close() throws IOException {
        if (storage.isClosed() || inMemory.isEmpty()) {
            return;
        }

        flush();
        storage.close();
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
}
