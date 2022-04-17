package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Storage {
    private volatile ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable = getNewMemTable();
    private volatile ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> readOnlyMemTable = getNewMemTable();

    public void beforeFlush() {
        this.readOnlyMemTable = this.memTable;
        this.memTable = getNewMemTable();
    }

    public void afterFlush() {
        this.readOnlyMemTable = getNewMemTable();
    }

    private static ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> getNewMemTable() {
        return new ConcurrentSkipListMap<>(Utils::compareMemorySegments);
    }

    public ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> getMemTable() {
        return memTable;
    }

    public ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> getReadOnlyMemTable() {
        return readOnlyMemTable;
    }
}
