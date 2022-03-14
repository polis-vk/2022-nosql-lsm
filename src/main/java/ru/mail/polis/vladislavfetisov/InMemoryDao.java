package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Config config;
    private final List<SSTable> tables = new ArrayList<>();
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = getStorage();

    public InMemoryDao(Config config) {
        this.config = config;
        tables.addAll(SSTable.getAllTables(config.basePath()));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> memory = fromMemory(from, to);
        Iterator<Entry<MemorySegment>> disc = tablesRange(from, to);

        return CustomIterators.mergeTwo(new CustomIterators.PeekingIterator<>(disc),
                new CustomIterators.PeekingIterator<>(memory));
    }

    private Iterator<Entry<MemorySegment>> tablesRange(MemorySegment from, MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(tables.size());
        for (SSTable table : tables) {
            iterators.add(table.range(from, to));
        }
        return CustomIterators.merge(iterators);
    }

    private Iterator<Entry<MemorySegment>> fromMemory(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return fullStorage();
        }
        if (from == null) {
            return to(to);
        }
        if (to == null) {
            return from(from);
        }
        return storage.subMap(from, to).values().iterator();
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

    private Iterator<Entry<MemorySegment>> from(MemorySegment from) {
        return storage.tailMap(from).values().iterator();
    }

    private Iterator<Entry<MemorySegment>> to(MemorySegment to) {
        return storage.headMap(to).values().iterator();
    }

    private Iterator<Entry<MemorySegment>> fullStorage() {
        return storage.values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        tables.add(writeSSTable());
    }

    private SSTable writeSSTable() throws IOException {
        Path tableName = config.basePath().resolve(String.valueOf(tables.size()));
        return SSTable.writeTable(tableName, storage.values());
    }

    private ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> getStorage() {
        return new ConcurrentSkipListMap<>(Utils::compareMemorySegments);
    }
}
