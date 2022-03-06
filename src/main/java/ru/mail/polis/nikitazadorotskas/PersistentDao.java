package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;


import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class PersistentDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> memory
            = new ConcurrentSkipListMap<>(Utils::compareMemorySegment);
    private final AtomicLong storageSizeInBytes = new AtomicLong(0);
    private MemorySegmentReader reader;
    private final Utils utils;

    public PersistentDao(Config config) {
        utils = new Utils(config);
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return getMap(from, to).values().iterator();
    }

    private ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> getMap(
            MemorySegment from, MemorySegment to
    ) {
        if (from == null && to == null) {
            return memory;
        }

        if (from == null) {
            return memory.headMap(to);
        }

        if (to == null) {
            return memory.tailMap(from);
        }

        return memory.subMap(from, to);
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) {
        BaseEntry<MemorySegment> result = memory.get(key);

        if (result != null) {
            return result;
        }

        if (reader == null) {
            reader = new MemorySegmentReader(utils);
        }

        return reader.getFromDisk(key);
    }

    @Override
    public void flush() throws IOException {
        utils.createFilesIfNotExist();

        MemorySegmentWriter segmentWriter = new MemorySegmentWriter(memory.size(), storageSizeInBytes.get(), utils);
        for (BaseEntry<MemorySegment> entry : memory.values()) {
            segmentWriter.writeEntry(entry);
        }

        memory.clear();
        segmentWriter.saveIndexes();
        segmentWriter.saveMemorySegments();
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        storageSizeInBytes.addAndGet(entry.key().byteSize() + entry.value().byteSize());
        memory.put(entry.key(), entry);
    }
}
