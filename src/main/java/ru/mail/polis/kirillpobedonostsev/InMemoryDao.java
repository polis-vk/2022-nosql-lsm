package ru.mail.polis.kirillpobedonostsev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final NavigableMap<MemorySegment, BaseEntry<MemorySegment>> map =
            new ConcurrentSkipListMap<>((e1, e2) -> Arrays.compare(e1.toByteArray(), e2.toByteArray()));

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return map.values().iterator();
        }
        NavigableMap<MemorySegment, BaseEntry<MemorySegment>> temp;
        if (from == null) {
            temp = map.headMap(to, false);
        } else if (to == null) {
            temp = map.tailMap(from, true);
        } else {
            temp = map.subMap(from, true, to, false);
        }
        return temp.values().iterator();
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) {
        return key == null ? null : map.get(key);
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        map.put(entry.key(), entry);
    }
}
