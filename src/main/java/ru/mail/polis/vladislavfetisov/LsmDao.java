package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class LsmDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Config config;
    private List<SSTable> tables;
    private final AtomicLong ssTableNum;
    private NavigableMap<MemorySegment, Entry<MemorySegment>> storage = getNewStorage();

    public LsmDao(Config config) {
        this.config = config;
        List<SSTable> fromDisc = SSTable.getAllTables(config.basePath());
        this.tables = fromDisc;
        if (fromDisc.isEmpty()) {
            ssTableNum = new AtomicLong(0);
            return;
        }
        String tableName = fromDisc.get(fromDisc.size() - 1).getTableName().getFileName().toString();
        ssTableNum = new AtomicLong(Long.parseLong(tableName) + 1);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return get(from, to, storage, tables);
    }

    private Iterator<Entry<MemorySegment>> get(MemorySegment from,
                                               MemorySegment to,
                                               NavigableMap<MemorySegment, Entry<MemorySegment>> storage,
                                               List<SSTable> tables) {

        Iterator<Entry<MemorySegment>> memory = fromMemory(from, to, storage);
        Iterator<Entry<MemorySegment>> disc = tablesRange(from, to, tables);

        PeekingIterator<Entry<MemorySegment>> merged = CustomIterators.mergeTwo(new PeekingIterator<>(disc),
                new PeekingIterator<>(memory));
        return CustomIterators.skipTombstones(merged);
    }

    private Iterator<Entry<MemorySegment>> tablesRange(MemorySegment from, MemorySegment to, List<SSTable> tables) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(tables.size());
        for (SSTable table : tables) {
            iterators.add(table.range(from, to));
        }
        return CustomIterators.merge(iterators);
    }

    private Iterator<Entry<MemorySegment>> fromMemory(MemorySegment from,
                                                      MemorySegment to,
                                                      NavigableMap<MemorySegment, Entry<MemorySegment>> storage) {
        if (from == null && to == null) {
            return storage.values().iterator();
        }
        return subMap(from, to).values().iterator();
    }

    private SortedMap<MemorySegment, Entry<MemorySegment>> subMap(MemorySegment from, MemorySegment to) {
        if (from == null) {
            return storage.headMap(to);
        }
        if (to == null) {
            return storage.tailMap(from);
        }
        return storage.subMap(from, to);
    }

    /**
     * Compact all SSTables.
     * It will work properly only if {@link #flush()} will be called after this method.
     */
    @Override
    public void compact() throws IOException {
        List<SSTable> fixed = this.tables;
        NavigableMap<MemorySegment, Entry<MemorySegment>> readOnlyStorage = this.storage;
        Iterator<Entry<MemorySegment>> all = get(null, null, readOnlyStorage, fixed);

        List<Entry<MemorySegment>> list = new ArrayList<>();
        all.forEachRemaining(list::add);

        this.tables = List.of(writeSSTable(list)); //immutable
        this.storage = getNewStorage();
        Utils.deleteTables(fixed);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> singleIterator = get(key, null);
        if (!singleIterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> desired = singleIterator.next();
        if (Utils.compareMemorySegments(desired.key(), key) != 0) {
            return null;
        }
        return desired;
    }

    @Override
    public void flush() throws IOException {
        if (storage.isEmpty()) {
            return;
        }
        SSTable table = writeSSTable(storage.values());
        tablesAtomicAdd(table); //need for concurrent get
        storage = getNewStorage();
    }

    private void tablesAtomicAdd(SSTable table) {
        ArrayList<SSTable> newTables = new ArrayList<>(tables.size() + 1);
        newTables.addAll(tables);
        newTables.add(table);
        tables = newTables;
    }

    private SSTable writeSSTable(Collection<Entry<MemorySegment>> values) throws IOException {
        Path tableName = nextTableName();
        return SSTable.writeTable(tableName, values);
    }

    private Path nextTableName() {
        return config.basePath().resolve(String.valueOf(ssTableNum.getAndIncrement()));
    }

    private static ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> getNewStorage() {
        return new ConcurrentSkipListMap<>(Utils::compareMemorySegments);
    }
}
