package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

public final class Comparator {

    private Comparator() {
    }

    public static int compare(MemorySegment o1, MemorySegment o2) {
        long offset = o1.mismatch(o2);

        if (offset == -1) {
            return 0;
        }

        if (o1.byteSize() == offset) {
            return -1;
        }

        if (o2.byteSize() == offset) {
            return 1;
        }
        return Byte.compare(MemoryAccess.getByteAtOffset(o1, offset),
                MemoryAccess.getByteAtOffset(o2, offset));
    }

    public static int iteratorsCompare(PeekIterator o1, PeekIterator o2) {
        int compare = Comparator.compare(o1.peek().key(), o2.peek().key());
        if (compare == 0) {
            return o1.getIndex() > o2.getIndex() ? 1 : -1;
        }
        return compare;
    }
}
