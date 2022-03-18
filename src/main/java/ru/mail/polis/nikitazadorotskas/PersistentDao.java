package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class PersistentDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> memory
            = new ConcurrentSkipListMap<>(this::compareMemorySegment);
    private final AtomicLong storageSizeInBytes = new AtomicLong(0);
    private final MemorySegmentReader reader;
    private final Utils utils;
    private final ResourceScope scope = ResourceScope.newSharedScope();

    public PersistentDao(Config config) throws IOException {
        MemorySegmentReader tmpReader;
        utils = new Utils(config);
        try {
            tmpReader = new MemorySegmentReader(utils, scope);
        } catch (NoSuchFileException e) {
            tmpReader = null;
        }
        reader = tmpReader;
    }

    private int compareMemorySegment(MemorySegment first, MemorySegment second) {
        return utils.compareMemorySegment(first, second);
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
    public BaseEntry<MemorySegment> get(@NotNull MemorySegment key) throws IOException {
        BaseEntry<MemorySegment> result = memory.get(key);

        if (result != null) {
            return result;
        }

        if (reader == null) {
            return null;
        }

        return reader.getFromDisk(key);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        storageSizeInBytes.addAndGet(entry.key().byteSize() + entry.value().byteSize());
        memory.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        if (!scope.isAlive()) {
            return;
        }
        scope.close();

        try (ResourceScope confinedScope = ResourceScope.newConfinedScope()) {
            utils.createFilesIfNotExist();
            MemorySegmentWriter segmentWriter
                    = new MemorySegmentWriter(memory.size(), storageSizeInBytes.get(), utils, confinedScope);
            for (BaseEntry<MemorySegment> entry : memory.values()) {
                segmentWriter.writeEntry(entry);
            }
        }

        memory.clear();
    }
}
