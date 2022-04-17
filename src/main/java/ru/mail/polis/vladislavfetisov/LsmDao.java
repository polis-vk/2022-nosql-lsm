package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemorySegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class LsmDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    public static final int THREADS = 2; //1 for compact
    public static final int EXCLUSIVE_PERMISSION = 1;
    private final Config config;
    private List<SSTable> ssTables;
    private final AtomicLong nextTableNum;
    private final AtomicLong memoryConsumption = new AtomicLong(0);
    private final ExecutorService service = Executors.newFixedThreadPool(THREADS);
    private final Storage storage = new Storage();
    private final AtomicBoolean isFlush = new AtomicBoolean();
    private final AtomicBoolean isCompact = new AtomicBoolean();
    private volatile List<SSTable> duringCompactionTables = new ArrayList<>();

    private final Semaphore semaphore = new Semaphore(EXCLUSIVE_PERMISSION);
    private final Logger logger = LoggerFactory.getLogger(LsmDao.class);

    /**
     * Get all files from dir(config.basePath), remove all files to file with suffix "compacted".
     * It's restricted, that amount of compacted files couldn't be more than 2.
     */
    public LsmDao(Config config) throws IOException {
        this.config = config;
        SSTable.Directory directory = SSTable.retrieveDir(config.basePath());
        List<SSTable> fromDisc = directory.ssTables();
        if (fromDisc.isEmpty()) {
            nextTableNum = new AtomicLong(0);
            this.ssTables = Collections.emptyList();
            return;
        }
        this.ssTables = fromDisc;
        if (directory.indexOfLastCompacted() != 0) {
            this.ssTables = fromDisc.subList(directory.indexOfLastCompacted(), fromDisc.size());
            Utils.deleteTablesToIndex(fromDisc, directory.indexOfLastCompacted());
        }
        String lastTable = ssTables.get(ssTables.size() - 1).getTableName().getFileName().toString();
        if (lastTable.endsWith(SSTable.COMPACTED)) {
            lastTable = Utils.removeSuffix(lastTable, SSTable.COMPACTED);
        }
        nextTableNum = new AtomicLong(Long.parseLong(lastTable) + 1);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return get(from, to, storage.getMemTable(), storage.getReadOnlyMemTable(), ssTables);
    }

    private Iterator<Entry<MemorySegment>> get(
            MemorySegment from,
            MemorySegment to,
            ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable,
            ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> readOnlyMemTable,
            List<SSTable> tables) {

        Iterator<Entry<MemorySegment>> memory = Utils.fromMemory(from, to, memTable);
        Iterator<Entry<MemorySegment>> readOnly = Utils.fromMemory(from, to, readOnlyMemTable);
        Iterator<Entry<MemorySegment>> disc = Utils.tablesRange(from, to, tables);

        PeekingIterator<Entry<MemorySegment>> merged = CustomIterators.mergeList(List.of(disc, readOnly, memory));
        return CustomIterators.skipTombstones(merged);
    }

    @Override
    public void compact() throws IOException {
        service.execute(() -> {
            synchronized (this) { //only one compact per time
                try {
                    logger.info("compact");
                    performCompact();
                } catch (IOException | InterruptedException e) {
                    logger.error("Compact is broken", e);
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    /**
     * Compact will be blocked until latest flush is done.
     */
    private void performCompact() throws IOException, InterruptedException {
        semaphore.acquire(EXCLUSIVE_PERMISSION);
        List<SSTable> fixed = this.ssTables;
        if (fixed.isEmpty()) {
            logger.info("Trying to compact empty ssTables");
            semaphore.release(EXCLUSIVE_PERMISSION);
            return;
        }
        duringCompactionTables = new ArrayList<>();
        duringCompactionTables.add(null);//for compacted table in future
        Path compactedPath = nextCompactedTable();
        isCompact.set(true);
        semaphore.release(EXCLUSIVE_PERMISSION);

        SSTable.Sizes sizes = Utils.getSizes(tablesFilteredFullRange(fixed));
        Iterator<Entry<MemorySegment>> forWrite = tablesFilteredFullRange(fixed);
        SSTable compacted = writeSSTable(compactedPath, forWrite, sizes.tableSize(), sizes.indexSize());

        synchronized (isCompact) { //sync between concurrent flush and compact
            duringCompactionTables.set(0, compacted);
            isCompact.set(false);
            this.ssTables = duringCompactionTables;
        }
        Utils.deleteTablesToIndex(fixed, fixed.size());
    }

    /**
     * We flush only readOnlyTable.
     */
    private void performFlush() throws IOException {
        if (storage.getReadOnlyMemTable().isEmpty()) {
            return;
        }
        SSTable.Sizes sizes = Utils.getSizes(storage.getReadOnlyMemTable().values().iterator());
        SSTable table = writeSSTable(
                nextOrdinaryTable(),
                storage.getReadOnlyMemTable().values().iterator(),
                sizes.tableSize(),
                sizes.indexSize()
        );
        synchronized (isCompact) { //sync between concurrent flush and compact
            if (isCompact.get()) {
                duringCompactionTables.add(table);
            }
            tablesAtomicAdd(table); //need for concurrent get
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        long size = Utils.sizeOfEntry(entry);
        if (memoryConsumption.addAndGet(size) > config.flushThresholdBytes()) {
            if (isFlush.get()) {
                throw new IllegalStateException("So many upserts");
            }
            if (semaphore.tryAcquire(EXCLUSIVE_PERMISSION)) {
                logger.info("program flush is started");
                asyncFlush(size);
            } else {
                logger.info("Program flush is rejected");
            }
        }
        storage.getMemTable().put(entry.key(), entry);
    }

    /**
     * If user's flush is running, then this method will be cancelled.
     */
    private void asyncFlush(long size) {
        storage.beforeFlush();
        memoryConsumption.set(size);
        isFlush.set(true);
        service.execute(() -> {
            try {
                performFlush();
                storage.afterFlush();
                logger.info("program flush is finished");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                semaphore.release(EXCLUSIVE_PERMISSION);
                isFlush.set(false);
            }
        });
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

    /**
     * Only one flush per time, this flush may be blocked until it could be performed.
     */
    @Override
    public void flush() throws IOException {
        try {
            logger.info("User want to flush");
            semaphore.acquire(EXCLUSIVE_PERMISSION);
            logger.info("User's flush is started");
            processFlush();
        } catch (InterruptedException e) {
            logger.error("Flush semaphore was interrupted", e);
            Thread.currentThread().interrupt();
        } finally {
            semaphore.release(EXCLUSIVE_PERMISSION);
            isFlush.set(false);
            logger.info("User's flush is finished");
        }
    }

    private void processFlush() throws IOException {
        isFlush.set(true);
        storage.beforeFlush();
        memoryConsumption.set(0);
        performFlush();
        storage.afterFlush();
    }

    private void tablesAtomicAdd(SSTable table) {
        ArrayList<SSTable> newTables = new ArrayList<>(ssTables.size() + 1);
        newTables.addAll(ssTables);
        newTables.add(table);
        ssTables = newTables;
    }

    @Override
    public void close() throws IOException {
        service.shutdown();
        try {
            if (!service.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Cant await termination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Cant await termination", e);
        }
        try {
            semaphore.acquire(EXCLUSIVE_PERMISSION);
        } catch (InterruptedException e) {
            logger.error("Cant take semaphore", e);
            Thread.currentThread().interrupt();
        }
        processFlush();
        for (SSTable table : ssTables) {
            table.close();
        }
        semaphore.release(EXCLUSIVE_PERMISSION);
        isFlush.set(false);
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

    private Iterator<Entry<MemorySegment>> tablesFilteredFullRange(List<SSTable> fixed) {
        Iterator<Entry<MemorySegment>> discIterator = Utils.tablesRange(null, null, fixed);
        return CustomIterators.skipTombstones(new PeekingIterator<>(discIterator));
    }

}
