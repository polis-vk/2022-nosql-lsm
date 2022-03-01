package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemorySegment;

import ru.mail.polis.Dao;
import ru.mail.polis.BaseEntry;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class MyMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final ConcurrentSkipListMap<MemorySegment, BaseEntry<MemorySegment>> data = new ConcurrentSkipListMap<>(
            (i, j) -> Arrays.compare(i.toByteArray(), j.toByteArray())
    );

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
    public void upsert(BaseEntry<MemorySegment> entry) {
        data.put(entry.key(), entry);
    }

    private Iterator<BaseEntry<MemorySegment>> shell(Map<MemorySegment, BaseEntry<MemorySegment>> sub) {
        return sub.values().iterator();
    }
}
