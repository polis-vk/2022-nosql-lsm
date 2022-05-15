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
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LsmDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    public static final int EXCLUSIVE_PERMISSION = 1;
    private final Config config;
    private long nextTableNum;
    private volatile boolean isCompact;
    private volatile boolean isClosed;
    private final ExecutorService flushExecutor
            = Executors.newSingleThreadExecutor(r -> new Thread(r, "flushThread"));
    private final ExecutorService compactExecutor
            = Executors.newSingleThreadExecutor(r -> new Thread(r, "compactThread"));
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
            this.nextTableNum = 0;
            this.storage = new Storage(Storage.Memory.getNewMemory(config.flushThresholdBytes()),
                    Storage.Memory.EMPTY_MEMORY, Collections.emptyList(), config);
            return;
        }
        List<SSTable> ssTables = fromDisc;
        if (directory.indexOfLastCompacted() != 0) {
            ssTables = fromDisc.subList(directory.indexOfLastCompacted(), fromDisc.size());
            Utils.deleteTablesToIndex(fromDisc, directory.indexOfLastCompacted());
        }
        this.nextTableNum = Utils.getLastTableNum(ssTables);
        this.storage = new Storage(Storage.Memory.getNewMemory(config.flushThresholdBytes()),
                Storage.Memory.EMPTY_MEMORY, ssTables, config);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        closeCheck();
        Storage fixedStorage = this.storage;
        PeekingIterator<Entry<MemorySegment>> merged = CustomIterators.getMergedIterator(from, to, fixedStorage);
        return CustomIterators.skipTombstones(merged);
    }

    @Override
    public void compact() throws IOException {
        if (isCompact) {
            return;
        }
        compactExecutor.execute(() -> {
            try {
                performCompact();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    /**
     * Compact will be blocked until latest flush is done.
     */
    private void performCompact() throws IOException {
        Path compactedPath = null;
        List<SSTable> fixed = null;
        try {
            flushSemaphore.acquire();
            fixed = this.storage.ssTables();
            closeCheck();
            if (fixed.isEmpty() || (fixed.size() == 1 && fixed.get(0).isCompacted())) {
                logger.info("Reject compact because it's redundant");
                return;
            }
            duringCompactionTables = new ArrayList<>();
            duringCompactionTables.add(null);//for compacted table in future
            compactedPath = nextCompactedTable();
            isCompact = true;
        } catch (InterruptedException e) {
            handleSemaphoreInterrupted(e);
        } finally {
            flushSemaphore.release();
        }
        SSTable.Sizes sizes = Utils.getSizes(Utils.tablesFilteredFullRange(fixed));
        Iterator<Entry<MemorySegment>> forWrite = Utils.tablesFilteredFullRange(fixed);
        SSTable compacted = SSTable.writeTable(compactedPath, forWrite, sizes.tableSize(), sizes.indexSize());

        synchronized (this) { //sync between concurrent flush and compact
            duringCompactionTables.set(0, compacted);
            isCompact = false;
            storage = storage.updateSSTables(duringCompactionTables);
        }
        Utils.deleteTablesToIndex(fixed, fixed.size());
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        boolean oversize;
        rwLock.readLock().lock();
        try {
            Storage localStorage = this.storage;
            if (localStorage.memory().isOversize().get() && localStorage.isFlushing()) { //if user's flush now running
                throw new IllegalStateException("So many upserts");
            }
            oversize = localStorage.memory().put(entry.key(), entry);
        } finally {
            rwLock.readLock().unlock();
        }
        if (oversize) {
            asyncFlush();
        }
    }

    private void asyncFlush() {
        flushExecutor.execute(() -> {
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
        try {
            flushSemaphore.acquire();
            closeCheck();
            logger.info("User's flush is started");
            processFlush();
            logger.info("User's flush is finished");
        } catch (InterruptedException e) {
            handleSemaphoreInterrupted(e);
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
        SSTable table = SSTable.writeTable(
                nextOrdinaryTable(),
                readOnlyMemTable.values().iterator(),
                sizes.tableSize(),
                sizes.indexSize()
        );
        synchronized (this) { //sync between concurrent flush and compact
            if (isCompact) {
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
        shutdownExecutor(flushExecutor);
        shutdownExecutor(compactExecutor);
        try {
            flushSemaphore.acquire();
            List<SSTable> ssTables = this.storage.ssTables();
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
        } catch (InterruptedException e) {
            handleSemaphoreInterrupted(e);
        } finally {
            flushSemaphore.release();
        }
    }

    private void shutdownExecutor(ExecutorService service) {
        service.shutdown();
        try {
            if (!service.awaitTermination(1, TimeUnit.HOURS)) {
                throw new IllegalStateException("Cant await termination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Cant await termination", e);
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

    private Path nextOrdinaryTable() {
        return nextTable(String.valueOf(nextTableNum++));
    }

    private Path nextCompactedTable() {
        return nextTable(nextTableNum++ + SSTable.COMPACTED);
    }

    private Path nextTable(String name) {
        return config.basePath().resolve(name);
    }

    private void closeCheck() {
        if (isClosed) {
            throw new IllegalStateException("Already closed");
        }
    }

    public Storage getStorage() {
        return storage;
    }

    private void handleSemaphoreInterrupted(InterruptedException e) {
        logger.error("Semaphore was interrupted", e);
        Thread.currentThread().interrupt();
    }
}
