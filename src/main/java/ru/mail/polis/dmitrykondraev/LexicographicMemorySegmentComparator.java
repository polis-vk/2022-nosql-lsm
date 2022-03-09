package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.util.Comparator;

/**
 * lexicographic comparison of UTF-8 strings can be done by byte, according to RFC 3239
 * (https://www.rfc-editor.org/rfc/rfc3629.txt, page 2)
 * this string comparison likely won't work with collation different from ASCII
 */
public class LexicographicMemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment lhs, MemorySegment rhs) {
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
        return Byte.compare(
                MemoryAccess.getByteAtOffset(lhs, offset),
                MemoryAccess.getByteAtOffset(rhs, offset)
        );
    }
}
