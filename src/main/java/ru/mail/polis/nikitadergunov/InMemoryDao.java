package ru.mail.polis.nikitadergunov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(InMemoryDao::comparator);

    private final Config config;
    private final WrapperReader readFromNonVolatileMemory;
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
        TreeSet<String> namesFiles = Files.list(config.basePath())
                .map(Path::toString)
                .filter(file -> file.endsWith(".dat") || file.endsWith(".ind"))
                .map(s -> s.substring(0, s.length() - 4))
                .collect(Collectors.toCollection(TreeSet::new));
        readFromNonVolatileMemory = new WrapperReader(namesFiles);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        lock.readLock().lock();
        try {
            Entry<MemorySegment> value = storage.get(key);
            if (value == null) {
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
                return readFromNonVolatileMemory.get(null, null,
                        storage.values().iterator());
            }
            if (from == null) {
                return readFromNonVolatileMemory.get(null, to,
                        storage.headMap(to).values().iterator());
            }
            if (to == null) {
                return readFromNonVolatileMemory.get(from, null,
                        storage.tailMap(from).values().iterator());
            }
            return readFromNonVolatileMemory.get(from, to,
                storage.subMap(from, to).values().iterator());
        } finally {
            lock.readLock().unlock();
        }

    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
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
    public void flush() throws IOException {
        lock.writeLock().lock();
        try {
            DataWriter writeToNonVolatileMemory =
                    new DataWriter(config, storage, sizeInBytes.get());
            writeToNonVolatileMemory.write();
            writeToNonVolatileMemory.close();
        } finally {
            lock.writeLock().lock();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
