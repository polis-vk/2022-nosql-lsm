package ru.mail.polis.alinashestakova;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final SortedMap<MemorySegment, BaseEntry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (storage.isEmpty()) {
            return Collections.emptyIterator();
        }

        if (from == null && to == null) {
            return storage.values().iterator();
        } else if (from == null) {
            return storage.headMap(to).values().iterator();
        } else if (to == null) {
            return storage.tailMap(from).values().iterator();
        }

        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(@NotNull BaseEntry<MemorySegment> entry) {
        if (entry == null) {
            return;
        }

        storage.put(entry.key(), entry);
    }

    public static class MemorySegmentComparator implements Comparator<MemorySegment> {

        @Override
        public int compare(MemorySegment o1, MemorySegment o2) {
            long offset = o1.mismatch(o2);
            if (offset == -1) {
                return 0;
            }

            Byte b = MemoryAccess.getByteAtOffset(o1, offset);
            return b.compareTo(MemoryAccess.getByteAtOffset(o2, offset));
        }
    }
}
