package ru.mail.polis.vladislavfetisov.wal;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Config;
import ru.mail.polis.Entry;
import ru.mail.polis.vladislavfetisov.Entries;
import ru.mail.polis.vladislavfetisov.Utils;
import ru.mail.polis.vladislavfetisov.lsm.LsmDao;
import ru.mail.polis.vladislavfetisov.lsm.SSTable;
import ru.mail.polis.vladislavfetisov.lsm.Storage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class WAL implements Closeable {
    private static final int BATCH_SIZE = 50;
    private static final long TIMEOUT_NANOS = TimeUnit.MICROSECONDS.toNanos(1);
    public static final String LOG = "log";
    public static final String LOG_DIR = LOG + File.separatorChar;
    public static final long POSITION_LENGTH = 2L * Long.BYTES;
    private final Path dir;
    private final Config config;
    private final ResourceScope writeScope = ResourceScope.newSharedScope();
    private volatile long recordsOffset = Long.BYTES;
    private volatile long positionsOffset;
    private volatile List<WriteOnlyLog> tables;
    private volatile List<WriteOnlyLog> tablesForDelete;
    private volatile WriteOnlyLog currentWriteLog;
    private long tableNum;
    public static final Logger LOGGER = LoggerFactory.getLogger(WAL.class);

    private WAL(Config config) {
        this.config = config;
        this.dir = config.basePath().resolve(LOG_DIR);
        this.tables = getNewTables();
        this.tablesForDelete = Collections.emptyList();
    }

    public static LogWithState getLogWithNewState(Config config, LsmDao.State initialState) throws IOException {
        LsmDao.State state = initialState;
        WAL wal = new WAL(config);
        try {
            Files.createDirectory(wal.dir);
        } catch (FileAlreadyExistsException e) {
            state = wal.restoreFromDir(state);
        }
        wal.currentWriteLog = wal.getNewWriteLog();
        return new LogWithState(wal, state);
    }

    private List<WriteOnlyLog> getNewTables() {
        return new ArrayList<>();
    }

    private WriteOnlyLog getNewWriteLog() throws IOException {
        return new WriteOnlyLog(nextLogName(), writeScope, config.flushThresholdBytes());
    }

    private Path nextLogName() {
        return dir.resolve(String.valueOf(tableNum++));
    }

    private LsmDao.State restoreFromDir(LsmDao.State initialState) throws IOException {
        LsmDao.State state = initialState;
        try (ResourceScope scopeForRead = ResourceScope.newSharedScope()) {
            List<ReadOnlyLog> logTables = ReadOnlyLog.getReadOnlyLogs(dir, scopeForRead);
            for (ReadOnlyLog logTable : logTables) {
                while (true) {
                    Storage storage = state.storage();
                    boolean oversize = false;
                    Entry<MemorySegment> entry;
                    while ((entry = logTable.peek()) != null) {
                        oversize = storage.memory().put(entry.key(), entry);
                        if (oversize) {
                            break;
                        }
                    }
                    if (!oversize) {
                        break;
                    }
                    state = refreshStateWithFlush(state);
                }
            }
            if (!state.storage().memory().isEmpty()) {
                state = refreshStateWithFlush(state);
            }
            for (ReadOnlyLog logTable : logTables) {
                Files.delete(logTable.getTableName());
                Files.delete(logTable.getPositionsName());
            }
            return state;
        }
    }

    private LsmDao.State refreshStateWithFlush(LsmDao.State state) throws IOException {
        String tableName = String.valueOf(state.nextTableNum());
        Storage newStorage = plainFlush(state.storage(), Utils.nextTable(tableName, config));
        return new LsmDao.State(newStorage, state.nextTableNum() + 1);
    }

    private static Storage plainFlush(Storage prevStorage, Path tableName) throws IOException {
        Storage storage = prevStorage;
        storage = storage.beforeFlush();
        Storage.Memory readOnlyMemTable = storage.readOnlyMemory();
        SSTable.Sizes sizes = Entries.getSizes(readOnlyMemTable.values().iterator());
        SSTable table = SSTable.writeTable(
                tableName,
                readOnlyMemTable.values().iterator(),
                sizes.tableSize(),
                sizes.indexSize()
        );
        List<SSTable> ssTables = storage.ssTables();
        ArrayList<SSTable> newTables = new ArrayList<>(ssTables.size() + 1);
        newTables.addAll(ssTables);
        newTables.add(table);
        storage = storage.updateSSTables(newTables);

        return storage.afterFlush();
    }

    public void beforeFlush() throws IOException {
        refreshWriteLog();
        this.tablesForDelete = this.tables;
        this.tables = getNewTables();
    }

    public void afterFlush() throws IOException {
        for (WriteOnlyLog localTable : this.tablesForDelete) {
            Files.delete(localTable.getTableName());
            Files.delete(localTable.getPositionsName());
        }
        this.tablesForDelete = Collections.emptyList();
    }

    public void put(Entry<MemorySegment> entry) throws IOException {
        if (!writeScope.isAlive()) {
            throw new IllegalStateException("WAL already closed");
        }
        long curRecordsOffset;
        long curPositionsOffset;
        WriteOnlyLog localWriteLog;
        synchronized (this) {
            localWriteLog = this.currentWriteLog;
            curRecordsOffset = recordsOffset;
            curPositionsOffset = positionsOffset;
            recordsOffset += Entries.sizeOfEntry(entry);
            positionsOffset += POSITION_LENGTH;
            if (recordsOffset >= localWriteLog.size()) {
                LOGGER.info("Need to flush");
                refreshWriteLog();
                localWriteLog = this.currentWriteLog;
                curRecordsOffset = Long.BYTES;
                curPositionsOffset = 0;
                recordsOffset = Entries.sizeOfEntry(entry) + Long.BYTES;
                positionsOffset = POSITION_LENGTH;
            }
            localWriteLog.setPositionsSize(curPositionsOffset + POSITION_LENGTH);
        }
        localWriteLog.putEntry(curRecordsOffset, entry);
        localWriteLog.putPositionChecksum(curPositionsOffset, Utils.getLongChecksum(curRecordsOffset));
        localWriteLog.putPosition(curPositionsOffset + Long.BYTES, curRecordsOffset);

        flush(localWriteLog);
    }

    private void flush(WriteOnlyLog localWriteLog) {
        WriteOnlyLog.LogState localState = localWriteLog.getState().get();
        boolean canFlush = localWriteLog.getPutCount().compareAndSet(BATCH_SIZE, 0);
        if (!canFlush) {
            localWriteLog.getPutCount().incrementAndGet();
            localState.getLock().lock();
            try {
                while (!localState.isFlushed()) {
                    long remaining = localState.getFlushCondition().awaitNanos(TIMEOUT_NANOS);
                    if (remaining > 0L) {
                        continue;
                    }
                    boolean weFlush = localWriteLog.flush(localState);
                    if (!weFlush && !localState.isFlushed()) {
                        localState.getFlushCondition().await();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                localState.getLock().unlock();
            }
            return;
        }
        localWriteLog.flush(localState);
    }

    private void refreshWriteLog() throws IOException {
        tables.add(currentWriteLog);
        currentWriteLog = getNewWriteLog();
    }

    @Override
    public void close() throws IOException {
        writeScope.close();
    }

    public record LogWithState(WAL log, LsmDao.State state) {
        //empty
    }
}
