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

public class MyMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private static final MemorySegment FIRST_KEY = MemorySegment.ofArray(new byte[]{});

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ConcurrentSkipListMap<MemorySegment, BaseEntry<MemorySegment>> data = new ConcurrentSkipListMap<>(
            Comparator::compare
    );
    private final Path basePath;
    private final FileWorker fileWorker = new FileWorker();

    public MyMemoryDao(Config config) {
        basePath = config.basePath();
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) throws IOException {
        lock.readLock().lock();
        try {
            List<PeekIterator> iterators = fileWorker.findEntries(from, to, basePath);

            MemorySegment newFrom = from;
            if (from == null) {
                newFrom = FIRST_KEY;
            }

            Iterator<BaseEntry<MemorySegment>> memoryIterator;
            if (to == null)  {
                memoryIterator = data.tailMap(newFrom).values().iterator();
            } else {
                memoryIterator = data.subMap(newFrom, to).values().iterator();
            }

            iterators.add(new PeekIterator(memoryIterator, iterators.size()));
            return new RangeIterator(iterators);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) throws IOException {
         lock.readLock().lock();
        try {
            BaseEntry<MemorySegment> result = data.get(key);
            if (result == null) {
                FileWorker reader = new FileWorker();
                result = reader.findEntry(key, basePath);
            }

            if (result != null && result.value() == null) {
                return null;
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        lock.writeLock().lock();
        try {
            data.put(entry.key(), entry);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (data.isEmpty()) {
                return;
            }
            fileWorker.writeEntries(data.values(), basePath);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
