package ru.mail.polis.dmitreemaximenko;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.util.Iterator;

public interface Table extends Iterable<Entry<MemorySegment>> {
    Iterator<Entry<MemorySegment>> get() throws IOException;
    Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) throws IOException;
}
