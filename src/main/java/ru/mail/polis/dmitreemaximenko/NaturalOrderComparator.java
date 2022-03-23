package ru.mail.polis.dmitreemaximenko;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import java.util.Comparator;

public class NaturalOrderComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment e1, MemorySegment e2) {
        long firstMismatch = e1.mismatch(e2);
        if (firstMismatch == -1) {
            return 0;
        }
        if (firstMismatch == e1.byteSize()) {
            return -1;
        }
        if (firstMismatch == e2.byteSize()) {
            return 1;
        }
        return Byte.compareUnsigned(
                MemoryAccess.getByteAtOffset(e1, firstMismatch),
                MemoryAccess.getByteAtOffset(e2, firstMismatch)
        );
    }
}

