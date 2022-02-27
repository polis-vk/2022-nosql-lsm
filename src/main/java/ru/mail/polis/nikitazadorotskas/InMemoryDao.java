package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, MemorySegment> memory
            = new ConcurrentSkipListMap<>(this::compareMemorySegment);

    private int compareMemorySegment(MemorySegment first, MemorySegment second) {
        long firstMismatchByte = first.mismatch(second);

        if (firstMismatchByte == -1) {
            return 0;
        }

        byte firstByte = MemoryAccess.getByteAtOffset(first, firstMismatchByte);
        byte secondByte = MemoryAccess.getByteAtOffset(second, firstMismatchByte);

        return Byte.compare(firstByte, secondByte);
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new BaseEntryIterator(getMap(from, to));
    }

    private ConcurrentNavigableMap<MemorySegment, MemorySegment> getMap(MemorySegment from, MemorySegment to) {
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

    private class BaseEntryIterator implements Iterator<BaseEntry<MemorySegment>> {
        private final Iterator<Entry<MemorySegment, MemorySegment>> iterator;

        private BaseEntryIterator(ConcurrentNavigableMap<MemorySegment, MemorySegment> map) {
            iterator = map.entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public BaseEntry<MemorySegment> next() {
            return toBaseEntry(iterator.next());
        }
    }

    private BaseEntry<MemorySegment> toBaseEntry(Entry<MemorySegment, MemorySegment> entry) {
        return new BaseEntry<>(entry.getKey(), entry.getValue());
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        memory.put(entry.key(), entry.value());
    }
}
