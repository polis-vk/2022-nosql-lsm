package ru.mail.polis.stepanponomarev.memtable;

import ru.mail.polis.stepanponomarev.OSXMemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class MemTable {
    private final SortedMap<OSXMemorySegment, TimestampEntry> store;
    private final FlushData flushData;

    public static final class FlushData {
        public final Iterator<TimestampEntry> data;
        public final long sizeBytes;
        public final int count;

        public FlushData(Iterator<TimestampEntry> flushData, long sizeBytes, int count) {
            this.data = flushData;
            this.sizeBytes = sizeBytes;
            this.count = count;
        }
    }

    public MemTable(SortedMap<OSXMemorySegment, TimestampEntry> store) {
        this.store = store;
        this.flushData = null;
    }

    private MemTable(SortedMap<OSXMemorySegment, TimestampEntry> store, FlushData flushData) {
        this.store = store;
        this.flushData = flushData;
    }

    public static MemTable createPreparedToFlush(MemTable memTable) {
        final SortedMap<OSXMemorySegment, TimestampEntry> clone = new ConcurrentSkipListMap<>();

        long sizeBytes = 0;
        final Iterator<TimestampEntry> timestampEntryIterator = memTable.get(null, null);
        while (timestampEntryIterator.hasNext()) {
            TimestampEntry entry = timestampEntryIterator.next();

            sizeBytes += Utils.sizeOf(entry);
            clone.put(entry.key(), entry);
        }

        final FlushData flushData = new FlushData(
                clone.values().iterator(),
                sizeBytes,
                clone.size()
        );

        return new MemTable(new ConcurrentSkipListMap<>(), flushData);
    }

    public static MemTable createFlushNullable(MemTable memTableWizard) {
        return new MemTable(memTableWizard.store, null);
    }

    public Iterator<TimestampEntry> get(OSXMemorySegment from, OSXMemorySegment to) {
        if (from == null && to == null) {
            return store.values().iterator();
        }

        if (from == null) {
            return store.headMap(to).values().iterator();
        }

        if (to == null) {
            return store.tailMap(from).values().iterator();
        }

        return store.subMap(from, to).values().iterator();
    }

    public TimestampEntry put(TimestampEntry entry) {
        return store.put(entry.key(), entry);
    }

    public FlushData getFlushData() {
        return flushData;
    }
}