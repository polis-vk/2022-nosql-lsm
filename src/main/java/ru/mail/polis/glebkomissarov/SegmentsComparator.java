package ru.mail.polis.glebkomissarov;

import org.jetbrains.annotations.NotNull;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import java.util.Comparator;

public class SegmentsComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(@NotNull MemorySegment o1, @NotNull MemorySegment o2) {
        long result = o1.mismatch(o2);
        return result == -1 ? 0 : Byte.compare(
                MemoryAccess.getByteAtOffset(o1, result), MemoryAccess.getByteAtOffset(o2, result)
        );
    }
}
