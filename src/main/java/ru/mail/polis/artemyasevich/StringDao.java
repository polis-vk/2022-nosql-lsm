package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StringDao implements Dao<String, BaseEntry<String>> {
    private final Config config;
    private final Storage storage;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicLong memoryUsage = new AtomicLong();
    private final Executor autoFlusher = Executors.newSingleThreadExecutor();
    private volatile ConcurrentNavigableMap<String, BaseEntry<String>> memory = new ConcurrentSkipListMap<>();
    private volatile ConcurrentNavigableMap<String, BaseEntry<String>> reserveMemory = new ConcurrentSkipListMap<>();

    public StringDao(Config config) throws IOException {
        this.config = config;
        this.storage = new Storage(config);
    }

    public StringDao() {
        config = null;
        storage = null;
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        lock.readLock().lock();
        List<PeekIterator> iterators = new ArrayList<>(2);
        try {
            if (to != null && to.equals(from)) {
                return Collections.emptyIterator();
            }
            iterators.add(new PeekIterator(getDataMapIterator(from, to), 0));
            if (storage != null) {
                iterators.add(new PeekIterator(storage.iterate(from, to), 1));
            }
        } finally {
            lock.readLock().unlock();
        }
        return new MergeIterator(iterators);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        lock.readLock().lock();
        try {
            BaseEntry<String> entry = memory.get(key);
            if (entry != null) {
                return entry.value() == null ? null : entry;
            }
            if (storage != null) {
                return storage.get(key);
            }
        } finally {
            lock.readLock().unlock();
        }
        return null;
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        if (config == null) {
            memory.put(entry.key(), entry);
            return;
        }
        lock.readLock().lock();
        try {
            long entrySize = EntryReadWriter.sizeOfEntry(entry);
            long currentMemoryUsage = memoryUsage.addAndGet(entrySize);
            if (currentMemoryUsage - entrySize > config.flushThresholdBytes()) {
                if (currentMemoryUsage > config.flushThresholdBytes() * 2) {
                    throw new IllegalStateException("Memory is full");
                }
                reserveMemory.put(entry.key(), entry);
                return;
            }
            if (currentMemoryUsage > config.flushThresholdBytes()) {
                autoFlusher.execute(() -> autoFlush(currentMemoryUsage - entrySize));
                reserveMemory.put(entry.key(), entry);
                return;
            }
            memory.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        if (storage == null) {
            return;
        }
        lock.writeLock().lock();
        try {
            Iterator<BaseEntry<String>> mergeIterator = get(null, null);
            if (!mergeIterator.hasNext()) {
                return;
            }
            storage.compact(mergeIterator);
            clearMemory();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        lock.writeLock().lock();
        try {
            flushMemoryIfNeeded();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (storage == null) {
            return;
        }
        lock.writeLock().lock();
        try {
            flushMemoryIfNeeded();
            storage.closeFiles();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void flushMemoryIfNeeded() throws IOException {
        if (storage == null) {
            return;
        }
        if (!memory.isEmpty()) {
            storage.flush(memory.values().iterator());
        }
        if (!reserveMemory.isEmpty()) {
            storage.flush(reserveMemory.values().iterator());
        }
        clearMemory();
    }

    private void clearMemory() {
        memory.clear();
        reserveMemory.clear();
        memoryUsage.set(0);
    }

    private void autoFlush(long memoryFlushed) {
        lock.writeLock().lock();
        try {
            if (storage == null || memory.isEmpty()) {
                return;
            }
            storage.flush(memory.values().iterator());
            memory.clear();
            ConcurrentNavigableMap<String, BaseEntry<String>> empty = memory;
            memory = reserveMemory; //memory и reserveMemory указывают на reserve map
            memoryUsage.addAndGet(-memoryFlushed); //Теперь upsertы пойдут по ссылке memory
            reserveMemory = empty;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Iterator<BaseEntry<String>> getDataMapIterator(String from, String to) {
        Map<String, BaseEntry<String>> subMap;
        if (from == null && to == null) {
            subMap = memory;
        } else if (from == null) {
            subMap = memory.headMap(to);
        } else if (to == null) {
            subMap = memory.tailMap(from);
        } else {
            subMap = memory.subMap(from, to);
        }
        return subMap.values().iterator();
    }

}
