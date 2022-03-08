package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class LsmDao implements Dao<ByteBuffer, Entry<ByteBuffer>> {
    private final String SS_TABLE_DIR_NAME = "table";

    private final Path path;
    private final List<SSTable> ssTables;
    private final SortedMap<ByteBuffer, Entry<ByteBuffer>> memTable = new ConcurrentSkipListMap<>();

    public LsmDao(Path bathPath) throws IOException {
        path = bathPath;
        ssTables = createSsTables(path);
    }

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        List<Iterator<Entry<ByteBuffer>>> iterators = new ArrayList<>();
        for (SSTable table : ssTables) {
            iterators.add(table.get(from, to));
        }

        if (from == null && to == null) {
            iterators.add(memTable.values().iterator());
        } else if (from == null) {
            iterators.add(memTable.headMap(to).values().iterator());
        } else if (to == null) {
            iterators.add(memTable.tailMap(from).values().iterator());
        } else {
            iterators.add(memTable.subMap(from, to).values().iterator());
        }

        return MergeIterator.instanceOf(iterators);
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        memTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        final Path ssTableDir = path.resolve(SS_TABLE_DIR_NAME + ssTables.size());
        Files.createDirectory(ssTableDir);

        final SSTable ssTable = new SSTable(ssTableDir);
        ssTable.flush(memTable.values().iterator());

        ssTables.add(ssTable);
        memTable.clear();
    }

    private List<SSTable> createSsTables(Path path) throws IOException {
        if (Files.notExists(path)) {
            return new ArrayList<>();
        }

        final String[] dirList = path.toFile().list();
        final int ssTableAmount = dirList == null ? 0 : dirList.length;
        final List<SSTable> tables = new ArrayList<>(ssTableAmount);
        for (int i = 0; i < ssTableAmount; i++) {
            tables.add(new SSTable(path.resolve(SS_TABLE_DIR_NAME + i)));
        }

        return tables;
    }
}
