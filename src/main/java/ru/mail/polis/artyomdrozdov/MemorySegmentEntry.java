package ru.mail.polis.artyomdrozdov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

import java.util.Map;

class MemorySegmentEntry implements Entry<MemorySegment> {

    private final MemorySegment entry;
    private final MemorySegment key;
    private final MemorySegment value;

    private MemorySegmentEntry(MemorySegment entry, MemorySegment key, MemorySegment value) {
        this.entry = entry;
        this.key = key;
        this.value = value;
    }
/*
    public static MemorySegmentEntry get(MemorySegment data, long offset) {
        long keySize = MemoryAccess.getLongAtOffset(data, offset);
        long valueOffset = offset + Long.BYTES + keySize;
        long valueSize = MemoryAccess.getLongAtOffset(data, valueOffset);

        return new MemorySegmentEntry();
    }
*/
    @Override
    public MemorySegment key() {
        return key;
    }

    @Override
    public MemorySegment value() {
        return value;
    }
}
