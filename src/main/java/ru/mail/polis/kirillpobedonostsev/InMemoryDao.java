package ru.mail.polis.kirillpobedonostsev;

import jdk.incubator.foreign.MemorySegment;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final NavigableMap<MemorySegment, BaseEntry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return map.values().iterator();
        } else if (from == null) {
            return map.headMap(to, false).values().iterator();
        } else if (to == null) {
            return map.tailMap(from, true).values().iterator();
        }
        return map.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public BaseEntry<MemorySegment> get(@NotNull MemorySegment key) {
        return key == null ? null : map.get(key);
    }

    @Override
    public void upsert(@NotNull BaseEntry<MemorySegment> entry) {
        map.put(entry.key(), entry);
    }
}
