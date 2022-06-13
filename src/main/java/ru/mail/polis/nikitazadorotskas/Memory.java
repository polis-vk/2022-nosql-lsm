package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class Memory {
    private final ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> map
            = new ConcurrentSkipListMap<>(this::compareMemorySegment);
    private final AtomicLong sizeInBytes = new AtomicLong(0);

    private int compareMemorySegment(MemorySegment first, MemorySegment second) {
        return Utils.compareMemorySegment(first, second);
    }

    PeekIterator getPeekIterator(
            int number,
            MemorySegment from,
            MemorySegment to
    ) {
        return new PeekIterator(number, getMap(from, to).values().iterator());
    }

    private ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> getMap(
            MemorySegment from, MemorySegment to
    ) {
        if (from == null && to == null) {
            return map;
        }

        if (from == null) {
            return map.headMap(to);
        }

        if (to == null) {
            return map.tailMap(from);
        }

        return map.subMap(from, to);
    }

    BaseEntry<MemorySegment> get(MemorySegment key) {
        return map.get(key);
    }

    Iterator<BaseEntry<MemorySegment>> getIterator() {
        return map.values().iterator();
    }

    boolean isEmpty() {
        return map.isEmpty();
    }

    int size() {
        return map.size();
    }

    void put(BaseEntry<MemorySegment> entry) {
        BaseEntry<MemorySegment> oldValue = map.get(entry.key());
        map.put(entry.key(), entry);
        if (oldValue != null) {
            sizeInBytes.addAndGet(-Utils.byteSizeOfEntry(oldValue));
        }
        sizeInBytes.addAndGet(Utils.byteSizeOfEntry(entry));
    }

    long getBytesSize() {
        return sizeInBytes.get();
    }
}
