package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Dao;
import ru.mail.polis.stepanponomarev.iterator.MergedIterator;
import ru.mail.polis.stepanponomarev.log.AsyncLogger;
import ru.mail.polis.stepanponomarev.memtable.MemTable;
import ru.mail.polis.stepanponomarev.memtable.MemTableProxy;
import ru.mail.polis.stepanponomarev.sstable.SSTable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class LsmDao implements Dao<OSXMemorySegment, TimestampEntry> {
    private static final long MAX_MEM_TABLE_SIZE_BYTES = (long) 2.5E8;
    private static final String SSTABLE_DIR_NAME = "SSTable_";

    private final Path path;
    private final AsyncLogger logger;

    private final AtomicLong currentSize = new AtomicLong();
    private final CopyOnWriteArrayList<SSTable> ssTables;

    private volatile MemTableProxy memTableProxy;

    public LsmDao(Path bathPath) throws IOException {
        if (Files.notExists(bathPath)) {
            throw new IllegalArgumentException("Path: " + bathPath + "is not exist");
        }

        path = bathPath;
        logger = new AsyncLogger(path, MAX_MEM_TABLE_SIZE_BYTES);
        memTableProxy = new MemTableProxy(
                createMemTable(logger.load())
        );
        ssTables = createStore(path);
    }

    private MemTable createMemTable(Iterator<TimestampEntry> load) {
        final SortedMap<OSXMemorySegment, TimestampEntry> store = new ConcurrentSkipListMap<>();
        while (load.hasNext()) {
            final TimestampEntry entry = load.next();
            store.put(entry.key(), entry);
        }

        return new MemTable(store);
    }

    @Override
    public Iterator<TimestampEntry> get(OSXMemorySegment from, OSXMemorySegment to) throws IOException {
        final List<Iterator<TimestampEntry>> iterators = new ArrayList<>();
        for (SSTable table : ssTables) {
            iterators.add(table.get(from, to));
        }

        MemTableProxy.FlushData flushData = memTableProxy.getFlushData();
        if (flushData != null) {
            iterators.add(flushData.data);
        }

        iterators.add(memTableProxy.get(from, to));

        return MergedIterator.instanceOf(iterators);
    }

    @Override
    public void upsert(TimestampEntry entry) {
        try {
            logger.log(entry);

            final long sizeBefore = currentSize.get();
            if (currentSize.get() >= MAX_MEM_TABLE_SIZE_BYTES) {
                flush();

                long size;
                do {
                    size = currentSize.get();
                } while (!currentSize.compareAndSet(size, size - sizeBefore));
            }

            memTableProxy.put(entry);
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

        memTableProxy = MemTableProxy.createPreparedToFlush(memTableProxy);
        MemTableProxy.FlushData flushData = memTableProxy.getFlushData();

        final Path dir = path.resolve(SSTABLE_DIR_NAME + timestamp);
        Files.createDirectory(dir);

        ssTables.add(SSTable.createInstance(dir, flushData.data, flushData.sizeBytes, flushData.count));

        memTableProxy = MemTableProxy.createFlushNullable(memTableProxy);
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
