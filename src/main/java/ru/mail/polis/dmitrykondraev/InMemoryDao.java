package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Author: Dmitry Kondraev
 */

public class InMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, MemorySegment> map = new ConcurrentSkipListMap<>((lhs, rhs) -> {
        // lexicographic comparison of UTF-8 strings can be done by byte, according to RFC 3239
        // (https://www.rfc-editor.org/rfc/rfc3629.txt, page 2)

        // this string comparison likely won't work with collation different from ASCII
        long offset = lhs.mismatch(rhs);
        if (offset == -1) {
            return 0;
        }
        if (offset == lhs.byteSize()) {
            return -1;
        }
        if (offset == rhs.byteSize()) {
            return 1;
        }
        return Byte.compare(MemoryAccess.getByteAtOffset(lhs, offset), MemoryAccess.getByteAtOffset(rhs, offset));
    });

    private static Iterator<BaseEntry<MemorySegment>> iterator(ConcurrentNavigableMap<MemorySegment, MemorySegment> map) {
        return map
                .entrySet()
                .stream()
                .map(e -> new BaseEntry<>(e.getKey(), e.getValue()))
                .iterator();
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return iterator(map);
        }
        if (from == null) {
            return iterator(map.headMap(to));
        }
        if (to == null) {
            return iterator(map.tailMap(from));
        }
        return iterator(map.subMap(from, to));
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        map.put(entry.key(), entry.value());
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) {
        MemorySegment value = map.get(key);
        if (value == null) {
            return null;
        }
        return new BaseEntry<>(key, value);
    }
}
