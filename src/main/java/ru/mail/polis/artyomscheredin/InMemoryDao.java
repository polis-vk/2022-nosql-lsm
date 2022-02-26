package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import jdk.incubator.foreign.MemorySegment;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final SortedMap<MemorySegment, BaseEntry<MemorySegment>> data = new ConcurrentSkipListMap<MemorySegment, BaseEntry<MemorySegment>>(new Comparator());

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (data.isEmpty()) {
            return Collections.emptyIterator();
        }

        if ((from == null) && (to == null)) {
            return data.values().iterator();
        } else if (from == null) {
            return data.headMap(to).values().iterator();
        } else if (to == null) {
            return data.tailMap(from).values().iterator();
        }
        return data.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        if (entry == null) {
            throw new IllegalArgumentException();
        }
        data.put(entry.key(), entry);
    }
}
