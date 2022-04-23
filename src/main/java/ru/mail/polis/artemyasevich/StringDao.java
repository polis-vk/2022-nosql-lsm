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
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StringDao implements Dao<String, BaseEntry<String>> {
    private final Config config;
    private final Storage storage;
    private final ReadWriteLock upsertLock = new ReentrantReadWriteLock();
    private final Lock storageLock = new ReentrantLock();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean autoFlushing = new AtomicBoolean();
    private MemoryState memoryState = MemoryState.newMemoryState();

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
        //issue: close() is not forbidden to close files due iterators process
        MemoryState memory = this.memoryState;
        List<PeekIterator> iterators = new ArrayList<>(3);
        if (to != null && to.equals(from)) {
            return Collections.emptyIterator();
        }
        if (!memory.memory.isEmpty()) {
            iterators.add(new PeekIterator(memoryIterator(from, to, memory.memory), 0));
        }
        if (!memory.flushing.isEmpty()) {
            iterators.add(new PeekIterator(memoryIterator(from, to, memory.flushing), 1));
        }
        if (storage != null) {
            iterators.add(new PeekIterator(storage.iterate(from, to), 2));
        }
        return new MergeIterator(iterators);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        MemoryState state = this.memoryState;
        BaseEntry<String> entry;
        entry = state.memory.get(key);
        if (entry == null && !state.flushing.isEmpty()) {
            entry = state.flushing.get(key);
        }
        if (entry == null && storage != null) {
            entry = storage.get(key);
        }
        return entry == null || entry.value() == null ? null : entry;
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        if (config == null || config.flushThresholdBytes() == 0) {
            memoryState.memory.put(entry.key(), entry);
            return;
        }
        upsertLock.readLock().lock();
        try {
            BaseEntry<String> previous = memoryState.memory.get(entry.key());
            long previousSize = previous == null ? 0 : EntryReadWriter.sizeOfEntry(previous);
            long entrySizeDelta = EntryReadWriter.sizeOfEntry(entry) - previousSize;
            long currentMemoryUsage = memoryState.memoryUsage.addAndGet(entrySizeDelta);
            if (currentMemoryUsage > config.flushThresholdBytes()) {
                if (currentMemoryUsage > config.flushThresholdBytes() * 2) {
                    executor.shutdown();
                    throw new IllegalStateException("Memory is full");
                }
                if (!autoFlushing.getAndSet(true)) {
                    executor.submit(this::flushMemory);
                }
            }
            memoryState.memory.put(entry.key(), entry);
        } finally {
            upsertLock.readLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        if (storage == null) {
            return;
        }
        storageLock.lock();
        try {
            executor.submit(() -> {
                storageLock.lock();
                try {
                    storage.compact();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } finally {
                    storageLock.unlock();
                }
            });
        } finally {
            storageLock.unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        flushMemory();
    }

    @Override
    public void close() throws IOException {
        if (storage == null) {
            return;
        }
        executor.shutdown();
        flushMemory();
        storage.close();
    }

    private void flushMemory() {
        if (storage == null) {
            return;
        }
        storageLock.lock();
        try {
            upsertLock.writeLock().lock();
            try {
                this.memoryState = memoryState.prepareForFlush();
            } finally {
                upsertLock.writeLock().unlock();
            }
            if (memoryState.flushing.isEmpty()) {
                return;
            }
            storage.flush(memoryState.flushing.values().iterator());
            upsertLock.writeLock().lock();
            try {
                this.memoryState = memoryState.afterFlush();
            } finally {
                upsertLock.writeLock().unlock();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            autoFlushing.set(false);
            storageLock.unlock();
        }
    }

    private Iterator<BaseEntry<String>> memoryIterator(String from, String to,
                                                       NavigableMap<String, BaseEntry<String>> map) {
        Map<String, BaseEntry<String>> subMap;
        if (from == null && to == null) {
            subMap = map;
        } else if (from == null) {
            subMap = map.headMap(to);
        } else if (to == null) {
            subMap = map.tailMap(from);
        } else {
            subMap = map.subMap(from, to);
        }
        return subMap.values().iterator();
    }

    private static class MemoryState {
        final ConcurrentNavigableMap<String, BaseEntry<String>> memory;
        final ConcurrentNavigableMap<String, BaseEntry<String>> flushing;
        final AtomicLong memoryUsage;

        private MemoryState(ConcurrentNavigableMap<String, BaseEntry<String>> memory,
                            ConcurrentNavigableMap<String, BaseEntry<String>> flushing) {
            this.memory = memory;
            this.flushing = flushing;
            this.memoryUsage = new AtomicLong();
        }

        private MemoryState(ConcurrentNavigableMap<String, BaseEntry<String>> memory,
                            ConcurrentNavigableMap<String, BaseEntry<String>> flushing,
                            AtomicLong memoryUsage) {
            this.memory = memory;
            this.flushing = flushing;
            this.memoryUsage = memoryUsage;
        }

        static MemoryState newMemoryState() {
            return new MemoryState(new ConcurrentSkipListMap<>(), new ConcurrentSkipListMap<>());
        }

        MemoryState prepareForFlush() {
            return new MemoryState(new ConcurrentSkipListMap<>(), memory, memoryUsage);
        }

        MemoryState afterFlush() {
            return new MemoryState(memory, new ConcurrentSkipListMap<>());
        }

    }

}
