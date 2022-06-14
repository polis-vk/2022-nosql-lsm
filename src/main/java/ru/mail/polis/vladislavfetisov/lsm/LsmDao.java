package ru.mail.polis.vladislavfetisov.lsm;

import jdk.incubator.foreign.MemorySegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.vladislavfetisov.Entries;
import ru.mail.polis.vladislavfetisov.MemorySegments;
import ru.mail.polis.vladislavfetisov.Utils;
import ru.mail.polis.vladislavfetisov.iterators.CustomIterators;
import ru.mail.polis.vladislavfetisov.iterators.PeekingIterator;
import ru.mail.polis.vladislavfetisov.wal.WAL;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LsmDao implements Dao<MemorySegment, Entry<MemorySegment>> {
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
    private final WAL log;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    public static final Logger LOGGER = LoggerFactory.getLogger(LsmDao.class);

    /**
     * Get all files from dir(config.basePath), remove all files to file with suffix "compacted".
     * It's restricted, that amount of compacted files couldn't be more than 2.
     */
    public LsmDao(Config config) throws IOException {
        this.config = config;
        SSTable.Directory directory = SSTable.retrieveDir(config.basePath());
        this.storage = Storage.getNewStorageWithSSTables(config, Collections.emptyList());
        List<SSTable> fromDisc = directory.ssTables();
        if (!fromDisc.isEmpty()) {
            List<SSTable> ssTables = fromDisc;
            if (directory.indexOfLastCompacted() != 0) {
                ssTables = fromDisc.subList(directory.indexOfLastCompacted(), fromDisc.size());
                Utils.deleteTablesToIndex(fromDisc, directory.indexOfLastCompacted());
            }
            this.nextTableNum = Utils.getLastTableNum(ssTables);
            this.storage = Storage.getNewStorageWithSSTables(config, ssTables);
        }

        State initialState = new State(storage, nextTableNum);
        WAL.LogWithState logWithState = WAL.getLogWithNewState(config, initialState);

        this.nextTableNum = logWithState.state().nextTableNum();
        this.storage = logWithState.state().storage();
        this.log = logWithState.log();
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
        Path compactedPath;
        List<SSTable> fixed;
        synchronized (flushExecutor) {
            closeCheck();
            fixed = this.storage.ssTables();
            if (fixed.isEmpty() || (fixed.size() == 1 && fixed.get(0).isCompacted())) {
                LOGGER.info("Reject compact because it's redundant");
                return;
            }
            duringCompactionTables = new ArrayList<>();
            duringCompactionTables.add(null);//for compacted table in future
            compactedPath = nextCompactedTable();
            isCompact = true;
        }

        SSTable.Sizes sizes = Entries.getSizes(Utils.tablesFilteredFullRange(fixed));
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
        closeCheck();
        boolean oversize;
        rwLock.readLock().lock();
        try {
            Storage localStorage = this.storage;
            if (localStorage.memory().isOversize().get() && localStorage.isFlushing()) {
                throw new IllegalStateException("So many upserts");
            }
            try {
                log.put(entry);
            } catch (IOException e) {
                LOGGER.error("WAL is broken");
                throw new UncheckedIOException(e);
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
            synchronized (flushExecutor) {
                try {
                    closeCheck();
                    LOGGER.info("Start program flush");
                    processFlush();
                    LOGGER.info("Program flush is finished");
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });
    }

    /**
     * Only one flush per time, this flush may be blocked until it could be performed.
     */
    @Override
    public void flush() throws IOException {
        LOGGER.info("User want to flush");
        if (storage.isFlushing()) {
            return;
        }
        synchronized (flushExecutor) {
            closeCheck();
            LOGGER.info("User's flush is started");
            processFlush();
            LOGGER.info("User's flush is finished");
        }
    }

    private void processFlush() throws IOException {
        rwLock.writeLock().lock();
        try {
            storage = storage.beforeFlush();
            log.beforeFlush();
        } finally {
            rwLock.writeLock().unlock();
        }
        performFlush();
        storage = storage.afterFlush();
        log.afterFlush();
    }

    /**
     * We flush only readOnlyTable.
     */
    private void performFlush() throws IOException {
        Storage.Memory readOnlyMemTable = storage.readOnlyMemory();
        if (readOnlyMemTable.isEmpty()) {
            return;
        }
        SSTable.Sizes sizes = Entries.getSizes(readOnlyMemTable.values().iterator());
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
        Utils.shutdownExecutor(flushExecutor);
        Utils.shutdownExecutor(compactExecutor);
        synchronized (flushExecutor) {
            if (isClosed) {
                LOGGER.info("Trying to close already closed storage");
                return;
            }
            isClosed = true;
            LOGGER.info("Closing storage");
            processFlush();
            for (SSTable table : this.storage.ssTables()) {
                table.close();
            }
            log.close();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> singleIterator = get(key, null);
        if (!singleIterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> desired = singleIterator.next();
        if (MemorySegments.compareMemorySegments(desired.key(), key) != 0) {
            return null;
        }
        return desired;
    }

    private Path nextOrdinaryTable() {
        return Utils.nextTable(String.valueOf(nextTableNum++), config);
    }

    private Path nextCompactedTable() {
        return Utils.nextTable(nextTableNum++ + SSTable.COMPACTED, config);
    }

    private void closeCheck() {
        if (isClosed) {
            throw new IllegalStateException("Already closed");
        }
    }

    public Storage getStorage() {
        return storage;
    }

    public record State(Storage storage, long nextTableNum) {
        //empty
    }
}
