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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StringDao implements Dao<String, BaseEntry<String>> {
    private final Config config;
    private final Storage storage;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock memoryLock = new ReentrantLock();
    private final Lock backgroundLock = new ReentrantLock();
    private final AtomicLong memoryUsage = new AtomicLong();
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
        memoryLock.lock(); //Хотелось бы дождаться фонового флаша
        List<PeekIterator> iterators = new ArrayList<>(3);
        try {
            if (to != null && to.equals(from)) {
                return Collections.emptyIterator();
            }
            if (!memory.isEmpty()) {
                iterators.add(new PeekIterator(memoryIterator(from, to), 0));
            }
            if (storage != null) {
                iterators.add(new PeekIterator(storage.iterate(from, to), 1));
            }
        } finally {
            memoryLock.unlock();
            lock.readLock().unlock();
        }
        return new MergeIterator(iterators);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        BaseEntry<String> entry;
        lock.readLock().lock();
        try {
            entry = memory.get(key);
            if (entry == null) {
                entry = reserveMemory.get(key);
            }
            if (entry == null && storage != null) {
                entry = storage.get(key);
            }
        } finally {
            lock.readLock().unlock();
        }
        return entry == null || entry.value() == null ? null : entry;
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        if (config == null || config.flushThresholdBytes() == 0) {
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
                new Thread(() -> autoFlush(currentMemoryUsage - entrySize)).start();
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
        new Thread(() -> {
            lock.readLock().lock();
            backgroundLock.lock(); //Делаем фоновый компакт, если не идёт фоновый флаш
            try {
                storage.compact();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                backgroundLock.unlock();
                lock.readLock().unlock();
            }
        }).start();
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
            storage.close();
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
        lock.readLock().lock();
        backgroundLock.lock(); //Не флашим, пока идёт компакт
        memoryLock.lock(); // Не флашим, пока есть итераторы по памяти
        try {
            if (storage == null || memory.isEmpty()) {
                return;
            }
            storage.flush(memory.values().iterator());
            memory.clear();
            ConcurrentNavigableMap<String, BaseEntry<String>> empty = memory;
            memory = reserveMemory;
            memoryUsage.addAndGet(-memoryFlushed); //Теперь upsertы пойдут на memory
            reserveMemory = empty;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            memoryLock.unlock();
            backgroundLock.unlock();
            lock.readLock().unlock();
        }
    }

    private Iterator<BaseEntry<String>> memoryIterator(String from, String to) {
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
