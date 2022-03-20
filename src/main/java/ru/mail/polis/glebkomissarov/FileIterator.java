package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileIterator implements Iterator<BaseEntry<MemorySegment>> {

    private final MemorySegment entries;
    private final MemorySegment offsets;

    private final long end;
    private long idx;

    public FileIterator(MemorySegment entries,
                           MemorySegment offsets,
                           long start, long end) {
        this.entries = entries;
        this.offsets = offsets;
        this.end = end;
        idx = start;
    }

    @Override
    public boolean hasNext() {
        return idx <= end;
    }

    @Override
    public BaseEntry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        MemorySegment key = entries.asSlice(
                MemoryAccess.getLongAtIndex(offsets, idx * 3),
                MemoryAccess.getLongAtIndex(offsets, idx * 3 + 1)
        );
        MemorySegment value = entries.asSlice(
                MemoryAccess.getLongAtIndex(offsets, idx * 3) + key.byteSize(),
                MemoryAccess.getLongAtIndex(offsets, idx * 3 + 2)
        );

        idx++;
        return new BaseEntry<>(key, value);
    }
}
