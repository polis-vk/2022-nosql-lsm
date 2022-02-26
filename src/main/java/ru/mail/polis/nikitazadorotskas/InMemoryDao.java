package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import jdk.internal.foreign.AbstractMemorySegmentImpl;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final ConcurrentNavigableMap<AbstractMemorySegmentImpl, AbstractMemorySegmentImpl> memory
            = new ConcurrentSkipListMap<>(this::compareMemorySegment);

    private int compareMemorySegment(AbstractMemorySegmentImpl first, AbstractMemorySegmentImpl second) {
        return Arrays.compare((byte[]) first.unsafeGetBase(), (byte[]) second.unsafeGetBase());
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return getIteratorFromMap(memory);
        }

        if (from == null) {
            return getIteratorFromMap(memory.headMap(cast(to)));
        }

        if (to == null) {
            return getIteratorFromMap(memory.tailMap(cast(from)));
        }

        return getIteratorFromMap(memory.subMap(cast(from), cast(to)));
    }

    private Iterator<BaseEntry<MemorySegment>> getIteratorFromMap(
            ConcurrentNavigableMap<AbstractMemorySegmentImpl, AbstractMemorySegmentImpl> map
    ) {
        return map.entrySet().stream()
                .map(this::toBaseEntry)
                .iterator();
    }

    private BaseEntry<MemorySegment> toBaseEntry(Map.Entry<AbstractMemorySegmentImpl, AbstractMemorySegmentImpl> entry) {
        return new BaseEntry<>(entry.getKey(), entry.getValue());
    }

    private AbstractMemorySegmentImpl cast(MemorySegment ms) {
        return (AbstractMemorySegmentImpl) ms;
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        memory.put(cast(entry.key()), (cast(entry.value())));
    }
}