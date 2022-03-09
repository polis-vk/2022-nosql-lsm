package ru.mail.polis.nikitadergunov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(InMemoryDao::comparator);
    private final Config config;
    private final ReadFromNonVolatileMemory readFromNonVolatileMemory;

    public static int comparator(MemorySegment firstSegment, MemorySegment secondSegment) {
        long offsetMismatch = firstSegment.mismatch(secondSegment);
        if (offsetMismatch == -1) {
            return 0;
        }
        return Byte.compare(MemoryAccess.getByteAtOffset(firstSegment, offsetMismatch),
                MemoryAccess.getByteAtOffset(secondSegment, offsetMismatch));
    }

    public InMemoryDao(Config config) {
        this.config = config;
        readFromNonVolatileMemory = new ReadFromNonVolatileMemory(config);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> value = storage.get(key);
        if (value != null) {
            return value;
        }
        if (readFromNonVolatileMemory.isExist()) {
            return readFromNonVolatileMemory.get(key);
        }
        return null;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
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
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        WriteToNonVolatileMemory writeToNonVolatileMemory = new WriteToNonVolatileMemory(config);
        writeToNonVolatileMemory.write(storage);
    }

}
