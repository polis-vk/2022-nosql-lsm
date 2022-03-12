package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Dao;
import ru.mail.polis.stepanponomarev.iterator.MergedIterator;
import ru.mail.polis.stepanponomarev.log.AsyncLogger;
import ru.mail.polis.stepanponomarev.sstable.SSTable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LsmDao implements Dao<OSXMemorySegment, EntryWithTime> {
    private static final long MAX_MEM_TABLE_SIZE_BYTES = 1_000_000;
    private static final String SSTABLE_DIR_NAME = "SSTable_";

    private final MemTable memTable;
    private final AsyncLogger logger;

    private final Path path;
    private final CopyOnWriteArrayList<SSTable> store;

    public LsmDao(Path bathPath) throws IOException {
        if (Files.notExists(bathPath)) {
            throw new IllegalArgumentException("Path: " + bathPath + "is not exist");
        }

        path = bathPath;
        logger = new AsyncLogger(path, MAX_MEM_TABLE_SIZE_BYTES);
        memTable = new MemTable(logger.load());
        store = createStore(path);
    }


    @Override
    public Iterator<EntryWithTime> get(OSXMemorySegment from, OSXMemorySegment to) throws IOException {
        final List<Iterator<EntryWithTime>> iterators = new ArrayList<>();
        for (SSTable table : store) {
            iterators.add(table.get(from, to));
        }

        iterators.add(memTable.get(from, to));

        return MergedIterator.instanceOf(iterators);
    }

    @Override
    public void upsert(EntryWithTime entry) {
        try {
            logger.log(entry);
            memTable.put(entry);

            if (memTable.sizeBytes() >= MAX_MEM_TABLE_SIZE_BYTES) {
                flush();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        logger.close();
        flush();
    }

    @Override
    public void flush() throws IOException {
        final long timestamp = System.nanoTime();

        final MemTable snapshot = memTable.getSnapshotAndClear();
        if (snapshot.isEmpty()) {
            return;
        }

        final Path dir = path.resolve(SSTABLE_DIR_NAME + timestamp);
        Files.createDirectory(dir);

        store.add(SSTable.createInstance(dir, snapshot.get(), snapshot.sizeBytes(), snapshot.size()));
        logger.clear(timestamp);
    }

    private CopyOnWriteArrayList<SSTable> createStore(Path path) throws IOException {
        try (Stream<Path> files = Files.list(path)) {
            final List<String> sstableNames = files
                    .map(f -> f.getFileName().toString())
                    .filter(n -> n.contains(SSTABLE_DIR_NAME))
                    .sorted()
                    .toList();

            final CopyOnWriteArrayList<SSTable> tables = new CopyOnWriteArrayList<>();
            for (String name : sstableNames) {
                tables.add(SSTable.upInstance(path.resolve(name)));
            }

            return tables;
        }
    }
}
