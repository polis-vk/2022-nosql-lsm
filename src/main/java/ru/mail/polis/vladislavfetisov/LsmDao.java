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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LsmDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    public static final int THREADS = 2; //1 for compact and 1 for flush
    public static final int EXCLUSIVE_PERMISSION = 1;
    private final Config config;
    private final AtomicLong nextTableNum;
    private final AtomicBoolean isCompact = new AtomicBoolean();
    private volatile boolean isClosed;
    private final ExecutorService service = Executors.newFixedThreadPool(THREADS);

    private volatile Storage storage;
    private volatile List<SSTable> duringCompactionTables = new ArrayList<>();

    private final Semaphore flushSemaphore = new Semaphore(EXCLUSIVE_PERMISSION);
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    public static final Logger logger = LoggerFactory.getLogger(LsmDao.class);

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
            storage = new Storage(Storage.Memory.getNewMemory(config.flushThresholdBytes()),
                    Storage.Memory.EMPTY_MEMORY, Collections.emptyList(), config);
            return;
        }
        List<SSTable> ssTables = fromDisc;
        if (directory.indexOfLastCompacted() != 0) {
            ssTables = fromDisc.subList(directory.indexOfLastCompacted(), fromDisc.size());
            Utils.deleteTablesToIndex(fromDisc, directory.indexOfLastCompacted());
        }
        this.nextTableNum = Utils.getLastTableNum(ssTables);
        storage = new Storage(Storage.Memory.getNewMemory(config.flushThresholdBytes()),
                Storage.Memory.EMPTY_MEMORY, ssTables, config);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        closeCheck();

        PeekingIterator<Entry<MemorySegment>> merged = getMergedIterator(from, to);
        return CustomIterators.skipTombstones(this, from, to, merged);
    }

    public PeekingIterator<Entry<MemorySegment>> getMergedIterator(
            MemorySegment from, MemorySegment to) {

        Storage fixedStorage = this.storage;
        List<SSTable> tables = fixedStorage.ssTables();

        Iterator<Entry<MemorySegment>> memory = fixedStorage.memory().get(from, to);
        Iterator<Entry<MemorySegment>> readOnly = fixedStorage.readOnlyMemory().get(from, to);
        Iterator<Entry<MemorySegment>> disc = Utils.tablesRange(from, to, tables);

        PeekingIterator<Entry<MemorySegment>> merged = CustomIterators.getMergedTwo(readOnly, memory);
        if (!tables.isEmpty()) {
            merged = CustomIterators.getMergedTwo(disc, merged);
        }
        return merged;
    }

    @Override
    public void compact() throws IOException {
        if (isCompact.get()) {
            return;
        }
        service.execute(() -> {
            synchronized (service) { //only one compact per time
                try {
                    performCompact();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });
    }

    /**
     * Compact will be blocked until latest flush is done.
     */

    private void performCompact() throws IOException {
        flushSemaphore.acquireUninterruptibly();
        Path compactedPath;
        List<SSTable> fixed;
        try {
            closeCheck();
            fixed = this.storage.ssTables();
            if (fixed.size() <= 1) {
                if (fixed.isEmpty()) {
                    logger.info("Trying to compact empty ssTables");
                    return;
                } else if (fixed.get(0).isCompacted()) {
                    logger.info("Trying to compact compacted table");
                    return;
                }
            }
            duringCompactionTables = new ArrayList<>();
            duringCompactionTables.add(null);//for compacted table in future
            compactedPath = nextCompactedTable();
            isCompact.set(true);
        } finally {
            flushSemaphore.release();
        }
        SSTable.Sizes sizes = Utils.getSizes(tablesFilteredFullRange(fixed));
        Iterator<Entry<MemorySegment>> forWrite = tablesFilteredFullRange(fixed);
        SSTable compacted = writeSSTable(compactedPath, forWrite, sizes.tableSize(), sizes.indexSize());

        synchronized (isCompact) { //sync between concurrent flush and compact
            duringCompactionTables.set(0, compacted);
            isCompact.set(false);
            storage = storage.updateSSTables(duringCompactionTables);
        }
        for (SSTable table : fixed) {
            table.close();
        }
        Utils.deleteTablesToIndex(fixed, fixed.size());
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        rwLock.readLock().lock();
        boolean oversize;
        Storage localStorage = this.storage;
        if (localStorage.memory().isOversize().get() && localStorage.isFlushing()) { //if user's flush now running
            throw new IllegalStateException("So many upserts");
        }
        try {
            oversize = this.storage.memory().put(entry.key(), entry);
        } finally {
            rwLock.readLock().unlock();
        }
        if (oversize) {
            asyncFlush();
        }
    }

    private void asyncFlush() {
        Storage localStorage = this.storage;
        if (localStorage.isFlushing()) {
            throw new IllegalStateException("So many upserts");
        }
        service.execute(() -> {
            try {
                if (flushSemaphore.tryAcquire()) {
                    try {
                        logger.info("Start program flush");
                        processFlush();
                        logger.info("Program flush is finished");
                    } finally {
                        flushSemaphore.release();
                    }
                } else {
                    logger.info("Race with user's flush");
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    /**
     * Only one flush per time, this flush may be blocked until it could be performed.
     */
    @Override
    public void flush() throws IOException {
        logger.info("User want to flush");
        flushSemaphore.acquireUninterruptibly();
        try {
            closeCheck();
            logger.info("User's flush is started");
            processFlush();
            logger.info("User's flush is finished");
        } finally {
            flushSemaphore.release();
        }
    }

    private void processFlush() throws IOException {
        rwLock.writeLock().lock();
        try {
            storage = storage.beforeFlush();
        } finally {
            rwLock.writeLock().unlock();
        }
        performFlush();
        storage = storage.afterFlush();
    }

    /**
     * We flush only readOnlyTable.
     */
    private void performFlush() throws IOException {
        Storage.Memory readOnlyMemTable = storage.readOnlyMemory();
        if (readOnlyMemTable.isEmpty()) {
            return;
        }
        SSTable.Sizes sizes = Utils.getSizes(readOnlyMemTable.values().iterator());
        SSTable table = writeSSTable(
                nextOrdinaryTable(),
                readOnlyMemTable.values().iterator(),
                sizes.tableSize(),
                sizes.indexSize()
        );
        synchronized (isCompact) { //sync between concurrent flush and compact
            if (isCompact.get()) {
                duringCompactionTables.add(table);
            }
            List<SSTable> ssTables = this.storage.ssTables();
            ArrayList<SSTable> newTables = new ArrayList<>(ssTables.size() + 1);
            newTables.addAll(ssTables);
            newTables.add(table);
            storage = storage.updateSSTables(newTables);
        }
    }

    @Override
    public void close() throws IOException {
        service.shutdown();
        try {
            if (!service.awaitTermination(1, TimeUnit.HOURS)) {
                throw new IllegalStateException("Cant await termination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Cant await termination", e);
        }
        flushSemaphore.acquireUninterruptibly();
        List<SSTable> ssTables = this.storage.ssTables();
        try {
            if (isClosed) {
                logger.info("Trying to close already closed storage");
                return;
            }
            isClosed = true;
            logger.info("Closing storage");
            processFlush();
            for (SSTable table : ssTables) {
                table.close();
            }
        } finally {
            flushSemaphore.release();
        }
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
        PeekingIterator<Entry<MemorySegment>> iterator = new PeekingIterator<>(discIterator);
        return CustomIterators.skipTombstones(this, null, null, iterator);
    }

    private void closeCheck() {
        if (isClosed) {
            throw new IllegalStateException("Already closed");
        }
    }
}
