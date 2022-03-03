package ru.mail.polis.artyomscheredin;

import jdk.incubator.foreign.MemorySegment;

public class MemSegComparator implements java.util.Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        return o1.asByteBuffer().compareTo(o2.asByteBuffer());
    }
}


