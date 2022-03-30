package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

public final class MemorySegmentEntry implements Entry<MemorySegment> {
    private final MemorySegment key;
    private final MemorySegment value;
    private final long byteSize;

    private MemorySegmentEntry(MemorySegment key, MemorySegment value, long byteSize) {
        this.key = key;
        this.value = value;
        this.byteSize = byteSize;
    }

    public static MemorySegmentEntry of(MemorySegment key, MemorySegment value) {
        return new MemorySegmentEntry(
                key,
                value,
                Long.BYTES + (value == null ? 0 : value.byteSize()) + key.byteSize());
    }

    public static MemorySegmentEntry of(MemorySegment entrySegment) {
        long valueSize = MemoryAccess.getIntAtOffset(entrySegment, 0L);
        return new MemorySegmentEntry(
                entrySegment.asSlice(Long.BYTES + Math.max(valueSize, 0)),
                valueSize < 0 ? null : entrySegment.asSlice(Long.BYTES, valueSize),
                entrySegment.byteSize());
    }

    @Override
    public MemorySegment key() {
        return key;
    }

    @Override
    public MemorySegment value() {
        return value;
    }

    public long bytesSize() {
        return byteSize;
    }

    public void copyTo(MemorySegment entrySegment) {
        MemoryAccess.setLongAtOffset(entrySegment, 0L, value == null ? -1 : value.byteSize());
        if (value != null) {
            entrySegment.asSlice(Long.BYTES, value.byteSize()).copyFrom(value);
        }
        entrySegment.asSlice(Long.BYTES + (value == null ? 0 : value().byteSize())).copyFrom(key);
    }
}
