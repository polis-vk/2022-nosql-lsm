package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.util.Comparator;
import java.util.Objects;

public final class OSXMemorySegment implements Comparable<OSXMemorySegment> {
    private static final Comparator<MemorySegment> comparator = (MemorySegment m1, MemorySegment m2) -> {
        final long mismatch = m1.mismatch(m2);
        if (mismatch == -1) {
            return 0;
        }

        if (mismatch == m1.byteSize()) {
            return -1;
        }

        if (mismatch == m2.byteSize()) {
            return 1;
        }

        return Byte.compare(
                MemoryAccess.getByteAtOffset(m1, mismatch),
                MemoryAccess.getByteAtOffset(m2, mismatch)
        );
    };

    private final MemorySegment memorySegment;

    public OSXMemorySegment(MemorySegment memorySegment) {
        this.memorySegment = memorySegment;
    }

    public MemorySegment getMemorySegment() {
        return memorySegment;
    }

    public long size() {
        return memorySegment.byteSize();
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                memorySegment.address().segmentOffset(memorySegment),
                memorySegment.byteSize()
        );
    }

    @Override
    public int compareTo(OSXMemorySegment o) {
        return comparator.compare(memorySegment, o.getMemorySegment());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof OSXMemorySegment) || hashCode() != obj.hashCode()) {
            return false;
        }

        return comparator.compare(memorySegment, ((OSXMemorySegment) obj).getMemorySegment()) == 0;
    }
}
