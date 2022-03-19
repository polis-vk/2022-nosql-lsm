package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.util.Comparator;

public class SegmentsComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        long result = o1.mismatch(o2);
        return result == -1 ? 0 : Byte.compare(
                MemoryAccess.getByteAtOffset(o1, result), MemoryAccess.getByteAtOffset(o2, result)
        );
    }
}
