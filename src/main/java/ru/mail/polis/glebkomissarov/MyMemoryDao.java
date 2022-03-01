package ru.mail.polis.glebkomissarov;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.jetbrains.annotations.Nullable;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

public class MyMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {

    private final ConcurrentSkipListMap<MemorySegment, BaseEntry<MemorySegment>> dataStore = new ConcurrentSkipListMap<>(
            (i, j) -> Arrays.compare(i.toByteArray(), j.toByteArray())
    );

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(@Nullable MemorySegment from, @Nullable MemorySegment to) {
        if (dataStore.isEmpty()) {
            return Collections.emptyIterator();
        }

        if (from == null) {
            return to == null ? shell(dataStore) : shell(dataStore.headMap(to));
        } else {
            return to == null ? shell(dataStore.tailMap(from)) : shell(dataStore.subMap(from, to));
        }
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        dataStore.put(entry.key(), entry);
    }

    private Iterator<BaseEntry<MemorySegment>> shell(Map<MemorySegment, BaseEntry<MemorySegment>> sub) {
        return sub.values().iterator();
    }
}
