package ru.mail.polis.nikitadergunov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(InMemoryDao::comparator);
    private final Config config;
    private final ReadFromNonVolatileMemory readFromNonVolatileMemory;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicLong sizeInBytes = new AtomicLong(0);

    public static int comparator(MemorySegment firstSegment, MemorySegment secondSegment) {
        long offsetMismatch = firstSegment.mismatch(secondSegment);
        if (offsetMismatch == -1) {
            return 0;
        }
        if (offsetMismatch == firstSegment.byteSize()) {
            return -1;
        }
        if (offsetMismatch == secondSegment.byteSize()) {
            return 1;
        }
        return Byte.compare(MemoryAccess.getByteAtOffset(firstSegment, offsetMismatch),
                MemoryAccess.getByteAtOffset(secondSegment, offsetMismatch));
    }

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        readFromNonVolatileMemory = new ReadFromNonVolatileMemory(config);
    }

    @Override
    public Entry<MemorySegment> get(@NotNull MemorySegment key) {
        lock.readLock().lock();
        try {
            Entry<MemorySegment> value = storage.get(key);
            if (value == null && readFromNonVolatileMemory.isExist()) {
                value = readFromNonVolatileMemory.get(key);
            }
            return value;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        lock.readLock().lock();
        try {
            if (from == null && to == null) {
                return storage.values().iterator();
            }
            if (from == null) {
                return storage.headMap(to).values().iterator();
            }
            if (to == null) {
                return storage.tailMap(from).values().iterator();
            }
            return storage.subMap(from, to).values().iterator();
        } finally {
            lock.readLock().unlock();
        }

    }

    @Override
    public void upsert(@NotNull Entry<MemorySegment> entry) {
        lock.readLock().lock();
        try {
            storage.put(entry.key(), entry);
            sizeInBytes.addAndGet(entry.key().byteSize());
            if (entry.value() != null) {
                sizeInBytes.addAndGet(entry.value().byteSize());
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        readFromNonVolatileMemory.close();
        lock.writeLock().lock();
        try {
            WriteToNonVolatileMemory writeToNonVolatileMemory =
                    new WriteToNonVolatileMemory(config, storage, sizeInBytes.get());
            writeToNonVolatileMemory.write();
            writeToNonVolatileMemory.close();
        } finally {
            lock.writeLock().lock();
        }
    }
}
