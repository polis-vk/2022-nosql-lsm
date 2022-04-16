package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class LsmDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Config config;
    private List<SSTable> tables;
    private final AtomicLong nextTableNum;
    private NavigableMap<MemorySegment, Entry<MemorySegment>> storage = getNewStorage();

    /**
     * Получаем все таблицы из директории, удаляем все до индекса последнего файла compacted,
     * больше 2 файлов compacted в директории быть не может.
     */
    public LsmDao(Config config) throws IOException {
        this.config = config;
        SSTable.Directory directory = SSTable.retrieveDir(config.basePath());
        List<SSTable> fromDisc = directory.ssTables();
        if (fromDisc.isEmpty()) {
            nextTableNum = new AtomicLong(0);
            this.tables = Collections.emptyList();
            return;
        }
        this.tables = fromDisc;
        if (directory.indexOfLastCompacted() != 0) {
            this.tables = fromDisc.subList(directory.indexOfLastCompacted(), fromDisc.size());
            Utils.deleteTablesToIndex(fromDisc, directory.indexOfLastCompacted());
        }
        String lastTable = tables.get(tables.size() - 1).getTableName().getFileName().toString();
        if (lastTable.endsWith(SSTable.COMPACTED)) {
            lastTable = Utils.removeSuffix(lastTable, SSTable.COMPACTED);
        }
        nextTableNum = new AtomicLong(Long.parseLong(lastTable) + 1);
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

    @Override
    public void compact() throws IOException {
        List<SSTable> fixed = this.tables;
        NavigableMap<MemorySegment, Entry<MemorySegment>> readOnlyStorage = this.storage;
        Iterator<Entry<MemorySegment>> forSize = get(null, null, readOnlyStorage, fixed);
        Iterator<Entry<MemorySegment>> forWrite = get(null, null, readOnlyStorage, fixed);

        SSTable.Sizes sizes = Utils.getSizes(forSize);

        this.tables = List.of(writeSSTable(
                nextCompactedTable(),
                forWrite,
                sizes.tableSize(),
                sizes.indexSize())
        );
        this.storage = getNewStorage();
        Utils.deleteTablesToIndex(fixed, fixed.size());
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
        NavigableMap<MemorySegment, Entry<MemorySegment>> readOnlyStorage = this.storage;
        SSTable.Sizes sizes = Utils.getSizes(readOnlyStorage.values().iterator());
        SSTable table = writeSSTable(
                nextOrdinaryTable(),
                readOnlyStorage.values().iterator(),
                sizes.tableSize(),
                sizes.indexSize()
        );

        tablesAtomicAdd(table); //need for concurrent get
        this.storage = getNewStorage();
    }

    private void tablesAtomicAdd(SSTable table) {
        ArrayList<SSTable> newTables = new ArrayList<>(tables.size() + 1);
        newTables.addAll(tables);
        newTables.add(table);
        tables = newTables;
    }

    @Override
    public void close() throws IOException {
        flush();
        for (SSTable table : tables) {
            table.close();
        }
    }

    private SSTable writeSSTable(Path table,
                                 Iterator<Entry<MemorySegment>> iterator,
                                 long tableSize,
                                 long indexSize) throws IOException {
        return SSTable.writeTable(table, iterator, tableSize, indexSize);
    }

    private Path nextOrdinaryTable() {
        return nextTable(String.valueOf(nextTableNum.getAndIncrement()));
    }

    private Path nextCompactedTable() {
        return nextTable(nextTableNum.getAndIncrement() + SSTable.COMPACTED);
    }

    private Path nextTable(String name) {
        return config.basePath().resolve(name);
    }

    private static ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> getNewStorage() {
        return new ConcurrentSkipListMap<>(Utils::compareMemorySegments);
    }
}
