package ru.mail.polis.stepanponomarev.store;

import ru.mail.polis.stepanponomarev.OSXMemorySegment;
import ru.mail.polis.stepanponomarev.TimestampEntry;
import ru.mail.polis.stepanponomarev.Utils;
import ru.mail.polis.stepanponomarev.iterator.MergedIterator;
import ru.mail.polis.stepanponomarev.sstable.SSTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

final class AtomicStore {
    private final List<SSTable> ssTables;
    private final Map<Long, FlushData> flushData;
    private final SortedMap<OSXMemorySegment, TimestampEntry> memTable;

    public AtomicStore(List<SSTable> ssTables, SortedMap<OSXMemorySegment, TimestampEntry> memTable) {
        this(ssTables, Collections.emptyMap(), memTable);
    }

    private AtomicStore(
            List<SSTable> ssTables,
            Map<Long, FlushData> flushData,
            SortedMap<OSXMemorySegment, TimestampEntry> memTable
    ) {
        this.ssTables = ssTables;
        this.flushData = flushData;
        this.memTable = memTable;
    }

    //TODO: Слишком много аллокаций
    public static AtomicStore prepareToFlush(AtomicStore flushStore, long timestamp) {
        if (flushStore.flushData.containsKey(timestamp)) {
            throw new IllegalStateException("Trying to flush already flushed data.");
        }

        if (flushStore.memTable.isEmpty()) {
            return new AtomicStore(
                    new ArrayList<>(flushStore.ssTables),
                    new ConcurrentSkipListMap<>(flushStore.memTable)
            );
        }

        long size = 0;
        final ConcurrentSkipListMap<OSXMemorySegment, TimestampEntry> flushingData = new ConcurrentSkipListMap<>();
        for (TimestampEntry entry : flushStore.memTable.values()) {
            size += Utils.sizeOf(entry);
            flushingData.put(entry.key(), entry);
        }

        final FlushData flushData = new FlushData(flushingData, size, flushingData.size());
        final Map<Long, FlushData> flushSnapshots = new HashMap<>(flushStore.flushData);
        flushSnapshots.put(timestamp, flushData);

        return new AtomicStore(
                flushStore.ssTables,
                flushSnapshots,
                filterByTimestamp(flushStore.memTable, timestamp)
        );
    }

    private static SortedMap<OSXMemorySegment, TimestampEntry> filterByTimestamp(
            SortedMap<OSXMemorySegment, TimestampEntry> source,
            long timestamp
    ) {
        return source.entrySet()
                .stream()
                .filter(e -> e.getValue().getTimestamp() > timestamp)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (o1, o2) -> o1,
                        ConcurrentSkipListMap<OSXMemorySegment, TimestampEntry>::new)
                );
    }

    public static AtomicStore afterFlush(AtomicStore flushStore, SSTable newSSTable, long timestamp) {
        final Map<Long, FlushData> flushSnapshots = new HashMap<>(flushStore.flushData);
        flushSnapshots.remove(timestamp);

        List<SSTable> newSSTables = new ArrayList<>(flushStore.ssTables);
        newSSTables.add(newSSTable);

        return new AtomicStore(newSSTables, flushSnapshots, new ConcurrentSkipListMap<>());
    }

    public SortedMap<OSXMemorySegment, TimestampEntry> getMemTable() {
        return memTable;
    }

    public FlushData getFlushData(long timestamp) {
        return flushData.get(timestamp);
    }

    public Iterator<TimestampEntry> get(OSXMemorySegment from, OSXMemorySegment to) {
        final List<Iterator<TimestampEntry>> data = new ArrayList<>(ssTables.size() + flushData.size() + 1);
        for (SSTable ssTable : ssTables) {
            data.add(ssTable.get(from, to));
        }

        for (FlushData fd : flushData.values()) {
            data.add(fd.get(from, to));
        }

        data.add(Utils.slice(memTable, from, to));

        return MergedIterator.instanceOf(data);
    }
}
