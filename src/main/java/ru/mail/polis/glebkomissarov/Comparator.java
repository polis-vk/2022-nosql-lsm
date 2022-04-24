package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.nio.file.Path;

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

    public static int pathsCompare(Path p1, Path p2) {
        String s1 = p1.toString();
        String s2 = p2.toString();

        if (s1.length() != s2.length()) {
            return s1.length() > s2.length() ? -1 : 1;
        }

        for (int i = 0; i < s1.length(); i++) {
            if (s1.charAt(i) != s2.charAt(i)) {
                return s1.charAt(i) > s2.charAt(i) ? -1 : 1;
            }
        }
        return 0;
    }
}
