package ru.mail.polis.stepanponomarev;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.stepanponomarev.sstable.SSTable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class LsmDao implements Dao<OSXMemorySegment, Entry<OSXMemorySegment>> {
    private static final String SSTABLE_DIR_NAME = "SSTable_";

    private final Path path;
    private final MemTable memTable;
    private final List<SSTable> store;

    public LsmDao(Path bathPath) throws IOException {
        path = bathPath;
        memTable = new MemTable();
        store = createStore(path);
    }

    @Override
    public Iterator<Entry<OSXMemorySegment>> get(OSXMemorySegment from, OSXMemorySegment to) throws IOException {
        final List<Iterator<Entry<OSXMemorySegment>>> iterators = new ArrayList<>();
        for (SSTable table : store) {
            iterators.add(table.get(from, to));
        }

        iterators.add(memTable.get(from, to));

        return MergedIterator.instanceOf(iterators);
    }

    @Override
    public void upsert(@NotNull Entry<OSXMemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        flush();
        for (SSTable table : store) {
            table.close();
        }
    }

    @Override
    public void flush() throws IOException {
        final Path dir = path.resolve(SSTABLE_DIR_NAME + store.size());
        if (Files.notExists(dir)) {
            Files.createDirectory(dir);
        }

        store.add(SSTable.createInstance(dir, memTable.get(), memTable.sizeBytes(), memTable.size()));
        memTable.clear();
    }

    private List<SSTable> createStore(Path path) throws IOException {
        if (Files.notExists(path)) {
            return new ArrayList<>();
        }

        try (Stream<Path> files = Files.list(path)) {
            final long ssTableCount = files
                    .map(f -> f.getFileName().toString())
                    .filter(n -> n.contains(SSTABLE_DIR_NAME))
                    .count();
            final List<SSTable> tables = new ArrayList<>();
            for (long i = 0; i < ssTableCount; i++) {
                tables.add(SSTable.upInstance(path.resolve(SSTABLE_DIR_NAME + i)));
            }

            return tables;
        }
    }
}
