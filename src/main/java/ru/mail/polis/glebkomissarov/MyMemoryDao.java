package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class MyMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final ConcurrentSkipListMap<MemorySegment, BaseEntry<MemorySegment>> data = new ConcurrentSkipListMap<>(
            new SegmentsComparator()
    );
    private final Path basePath;
    private final AtomicLong finalSize = new AtomicLong(0L);

    public MyMemoryDao(Config config) {
        basePath = config.basePath();
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (data.isEmpty()) {
            return Collections.emptyIterator();
        }

        if (from == null) {
            return to == null ? shell(data) : shell(data.headMap(to));
        } else {
            return to == null ? shell(data.tailMap(from)) : shell(data.subMap(from, to));
        }
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) throws IOException {
        BaseEntry<MemorySegment> result = data.get(key);

        if (result == null) {
            result = Converter.searchEntry(basePath, key);
        }
        return result;
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        finalSize.addAndGet(entry.key().byteSize() + entry.value().byteSize());
        data.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        Converter.startSerializeEntries(data.values(), finalSize.get(), basePath);
        for (BaseEntry<MemorySegment> entry : data.values()) {
            Converter.writeEntries(entry, entry.key().byteSize(), entry.value().byteSize());
        }
        Converter.writeOffsets();
    }

    private Iterator<BaseEntry<MemorySegment>> shell(Map<MemorySegment, BaseEntry<MemorySegment>> sub) {
        return sub.values().iterator();
    }
}
