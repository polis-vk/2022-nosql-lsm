package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemorySegment;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

class MemoryTable {
    private final AtomicLong byteSize;
    private final ConcurrentNavigableMap<MemorySegment, MemorySegmentEntry> map;
    final MemoryTable previous;

    MemoryTable(AtomicLong byteSize, ConcurrentNavigableMap<MemorySegment, MemorySegmentEntry> map, MemoryTable previous) {
        this.byteSize = byteSize;
        this.map = map;
        this.previous = previous;
    }

    /**
     * @param entry inserted entry
     * @return byteSize after upsert
     */
    public long upsert(MemorySegmentEntry entry) {
        // implicit check for non-null entry and entry.key()
        map.put(entry.key(), entry);
        return byteSize.addAndGet(entry.bytesSize());
    }

    public MemorySegmentEntry get(MemorySegment key) {
        MemorySegmentEntry entry = map.get(key);
        if ((entry == null || entry.isTombStone()) && previous != null) {
            return previous.get(key);
        }
        return entry;
    }

    public Iterator<MemorySegmentEntry> get(MemorySegment from, MemorySegment to) {
        Map<MemorySegment, MemorySegmentEntry> subMap = to == null ? map.tailMap(from) : map.subMap(from, to);
        return iterator(subMap);
    }

    public boolean isEmpty() {
        return map.isEmpty() || (previous != null && previous.isEmpty());
    }

    public Collection<MemorySegmentEntry> values() {
        if (previous != null) {
            throw new IllegalStateException("Can't ask for values with previous state");
        }
        return map.values();
    }

    public MemoryTable dropPrevious() {
        return new MemoryTable(byteSize, map, null);
    }

    public MemoryTable forward() {
        if (previous != null) {
            throw new IllegalStateException("Can't forward with previous state");
        }
        return new MemoryTable(
                new AtomicLong(),
                new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE),
                this);
    }

    public static MemoryTable of() {
        return new MemoryTable(
                new AtomicLong(),
                new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE),
                null);
    }

    private static <K, V> Iterator<V> iterator(Map<K, V> map) {
        return map.values().iterator();
    }
}
