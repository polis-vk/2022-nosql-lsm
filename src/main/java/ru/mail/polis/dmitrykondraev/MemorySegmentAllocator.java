package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemorySegment;

import java.io.IOException;

@FunctionalInterface
public interface MemorySegmentAllocator {
    MemorySegment allocate(long size) throws IOException;
}
