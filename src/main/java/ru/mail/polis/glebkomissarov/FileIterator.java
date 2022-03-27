package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileIterator implements Iterator<BaseEntry<MemorySegment>> {

    private static final int FROM_OFFSET_TO_IDX = 3;
    private static final int WRONG_SIZE = -1;

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

        long keyOffset = MemoryAccess.getLongAtIndex(offsets, idx * FROM_OFFSET_TO_IDX);
        long keySize = MemoryAccess.getLongAtIndex(offsets, idx * FROM_OFFSET_TO_IDX + 1);

        MemorySegment key = entries.asSlice(keyOffset, keySize);

        MemorySegment value;
        long valueSize = MemoryAccess.getLongAtIndex(offsets, idx * FROM_OFFSET_TO_IDX + 2);
        if (valueSize != WRONG_SIZE) {
            value = entries.asSlice(keyOffset + key.byteSize(), valueSize);
        } else {
            value = null;
        }

        idx++;
        return new BaseEntry<>(key, value);
    }
}
