package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class MyMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final ConcurrentSkipListMap<MemorySegment, BaseEntry<MemorySegment>> data = new ConcurrentSkipListMap<>(
            (i, j) -> {
                long result = i.mismatch(j);
                return result == -1 ? 0 : Byte.compare(
                        MemoryAccess.getByteAtOffset(i, result), MemoryAccess.getByteAtOffset(j, result)
                );
            }
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
    public BaseEntry<MemorySegment> get(@NotNull MemorySegment key) {
        return data.get(key);
    }

    @Override
    public void upsert(@NotNull BaseEntry<MemorySegment> entry) {
        data.put(entry.key(), entry);
    }

    private Iterator<BaseEntry<MemorySegment>> shell(Map<MemorySegment, BaseEntry<MemorySegment>> sub) {
        return sub.values().iterator();
    }
}
