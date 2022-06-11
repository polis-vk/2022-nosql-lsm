package ru.mail.polis.vladislavfetisov.wal;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemoryHandles;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Config;
import ru.mail.polis.Entry;
import ru.mail.polis.vladislavfetisov.Entries;
import ru.mail.polis.vladislavfetisov.MemorySegments;
import ru.mail.polis.vladislavfetisov.Utils;
import ru.mail.polis.vladislavfetisov.lsm.LsmDao;
import ru.mail.polis.vladislavfetisov.lsm.SSTable;
import ru.mail.polis.vladislavfetisov.lsm.Storage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class WAL implements Closeable {
    private static final int BATCH_SIZE = 500;
    public static final String LOG = "log";
    public static final String LOG_DIR = LOG + File.separatorChar;
    private static final long POSITION_LENGTH = 2L * Long.BYTES;
    private final Path dir;
    private final Config config;
    private final ResourceScope writeScope = ResourceScope.newSharedScope();
    private volatile long recordsOffset = Long.BYTES;
    private volatile long positionsOffset = 0;
    private volatile List<WriteOnlyLog> tables;
    private volatile List<WriteOnlyLog> tablesForDelete;
    private volatile WriteOnlyLog currentWriteLog;
    private long tableNum = 0;
    public static final Logger LOGGER = LoggerFactory.getLogger(WAL.class);

    private WAL(Config config) {
        this.config = config;
        this.dir = config.basePath().resolve(LOG_DIR);
        this.tables = getNewTables();
        this.tablesForDelete = Collections.emptyList();
    }

    public static LogWithState getLogWithNewState(Config config, LsmDao.State state) throws IOException {
        WAL wal = new WAL(config);
        try {
            Files.createDirectory(wal.dir);
        } catch (FileAlreadyExistsException e) {
            state = wal.restoreFromDir(state);
        }
        wal.currentWriteLog = wal.getNewWriteLog();
        return new LogWithState(wal, state);
    }

    private ArrayList<WriteOnlyLog> getNewTables() {
        return new ArrayList<>();
    }

    private WriteOnlyLog getNewWriteLog() throws IOException {
        return new WriteOnlyLog(nextLogName(), writeScope, config.flushThresholdBytes());
    }

    private Path nextLogName() {
        return dir.resolve(String.valueOf(tableNum++));
    }

    private LsmDao.State restoreFromDir(LsmDao.State state) throws IOException {
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
                Files.delete(logTable.tableName);
                Files.delete(logTable.positionsName);
            }
            return state;
        }
    }

    private LsmDao.State refreshStateWithFlush(LsmDao.State state) throws IOException {
        String tableName = String.valueOf(state.nextTableNum());
        Storage newStorage = plainFlush(state.storage(), Utils.nextTable(tableName, config));
        return new LsmDao.State(newStorage, state.nextTableNum() + 1);
    }

    /**
     * Not thread safe
     */
    private static Storage plainFlush(Storage storage, Path tableName) throws IOException {
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

    /**
     * Зафиксировать таблицы
     */
    public void beforeFlush() throws IOException {
        addWriteLogToTables();
        this.tablesForDelete = this.tables;
        this.tables = getNewTables();
    }

    /**
     * Удалить ненужные таблицы
     */
    public void afterFlush() throws IOException {
        for (WriteOnlyLog localTable : this.tablesForDelete) {
            Files.delete(localTable.tableName);
            Files.delete(localTable.positionsName);
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
                addWriteLogToTables();
                localWriteLog = this.currentWriteLog;
                curRecordsOffset = Long.BYTES;
                curPositionsOffset = 0;
                recordsOffset = Entries.sizeOfEntry(entry) + Long.BYTES;
                positionsOffset = POSITION_LENGTH;
            }
        }
        localWriteLog.putEntry(curRecordsOffset, entry);
        localWriteLog.putPositionChecksum(curPositionsOffset, Utils.getLongChecksum(curRecordsOffset));
        localWriteLog.putPosition(curPositionsOffset + Long.BYTES, curRecordsOffset);

        boolean canFlush = localWriteLog.putCount.compareAndSet(BATCH_SIZE, 0);
        if (!canFlush) {
            localWriteLog.putCount.incrementAndGet();
            return;
        }
        boolean success = localWriteLog.trySetPositionsSize(curPositionsOffset + POSITION_LENGTH);
        if (success) {
            localWriteLog.flush();
        }
    }


    private void addWriteLogToTables() throws IOException {
        tables.add(currentWriteLog);
        currentWriteLog = getNewWriteLog();
    }

    @Override
    public void close() throws IOException {
        writeScope.close();
    }

    /**
     * Not thread safe
     */
    private static class ReadOnlyLog {
        private final MemorySegment recordsFile;
        private final MemorySegment positionsFile;

        private final Path tableName;
        private final Path positionsName;

        private long positionsOffset = 0;

        private ReadOnlyLog(Path tableName, ResourceScope scope) throws IOException {
            this.tableName = tableName;
            this.positionsName = Path.of(tableName + SSTable.INDEX);
            this.recordsFile
                    = MemorySegments.map(tableName, Files.size(tableName), FileChannel.MapMode.READ_ONLY, scope);
            long positionsSize = MemoryAccess.getLongAtOffset(recordsFile, 0);
            this.positionsFile =
                    MemorySegments.map(positionsName, positionsSize, FileChannel.MapMode.READ_ONLY, scope);
        }

        public boolean isEmpty() {
            return positionsOffset == positionsFile.byteSize();
        }

        public Entry<MemorySegment> peek() {
            if (isEmpty()) {
                return null;
            }
            while (true) {
                long positionChecksum = MemoryAccess.getLongAtOffset(positionsFile, positionsOffset);
                long position = MemoryAccess.getLongAtOffset(positionsFile, positionsOffset + Long.BYTES);
                positionsOffset += POSITION_LENGTH;
                if (positionChecksum != Utils.getLongChecksum(position)) {
                    LOGGER.info("Checksum is not valid");
                    if (isEmpty()) {
                        return null;
                    }
                    continue;
                }
                return MemorySegments.readEntryByOffset(recordsFile, position);
            }
        }

        public static List<ReadOnlyLog> getReadOnlyLogs(Path dir, ResourceScope scope) throws IOException {
            try (Stream<Path> files = Files.list(dir)) {
                return files
                        .filter(path -> {
                            String s = path.toString();
                            return !s.endsWith(SSTable.INDEX);
                        })
                        .mapToInt(Utils::getTableNum)
                        .sorted()
                        .mapToObj(i -> {
                            try {
                                return new ReadOnlyLog(dir.resolve(String.valueOf(i)), scope);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        })
                        .toList();
            }
        }
    }

    private static class WriteOnlyLog {
        private final VarHandle handle = MemoryHandles.varHandle(long.class, 1, ByteOrder.nativeOrder());
        private final AtomicInteger putCount = new AtomicInteger();
        private final MemorySegment recordsFile;
        private final MemorySegment positionsFile;
        private final Path tableName;
        private final Path positionsName;

        public long size() {
            return recordsFile.byteSize();
        }

        public WriteOnlyLog(Path tableName, ResourceScope scope, long length) throws IOException {
            this.tableName = tableName;
            this.positionsName = Path.of(tableName + SSTable.INDEX);
            Utils.newFile(tableName);
            Utils.newFile(positionsName);
            this.recordsFile = MemorySegments.map(tableName, length, FileChannel.MapMode.READ_WRITE, scope);
            long maxOffsetsLength = ((length - Long.BYTES) / Entries.MIN_LENGTH) * POSITION_LENGTH;
            this.positionsFile =
                    MemorySegments.map(positionsName, maxOffsetsLength, FileChannel.MapMode.READ_WRITE, scope);
        }

        public void putEntry(long offset, Entry<MemorySegment> entry) {
            MemorySegments.writeEntry(recordsFile, offset, entry);
        }

        public boolean trySetPositionsSize(long value) {
            long prev = (long) handle.getVolatile(recordsFile, 0);
            if (prev > value) {
                return false;
            }
            handle.setVolatile(recordsFile, 0, value);
            return true;
        }

        public void putPosition(long offset, long value) {
            MemoryAccess.setLongAtOffset(positionsFile, offset, value);
        }

        public void putPositionChecksum(long offset, long checksum) {
            MemoryAccess.setLongAtOffset(positionsFile, offset, checksum);
        }

        public void flush() {
            recordsFile.force();
            positionsFile.force();
        }
    }

    public record LogWithState(WAL log, LsmDao.State state) {

    }
}
