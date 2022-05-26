package ru.mail.polis.vladislavfetisov.wal;

import jdk.incubator.foreign.MemoryAccess;
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
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WAL implements Closeable {
    public static final String LOG = "log";
    public static final String LOG_DIR = LOG + File.separatorChar;
    private static final long POSITION_LENGTH = 2L * Long.BYTES;
    private final Path dir;
    private final Config config;
    private final ResourceScope writeScope = ResourceScope.newSharedScope();
    private volatile long recordsOffset = 0;
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
                    String tableName = String.valueOf(state.nextTableNum());
                    Storage newStorage = plainFlush(storage, Utils.nextTable(tableName, config));
                    state = new LsmDao.State(newStorage, state.nextTableNum() + 1);
                }
                Files.delete(logTable.tableName);
                Files.delete(logTable.positionsName);
            }
            return state;
        }
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

        storage.ssTables().add(table);

        return storage.afterFlush();
    }

    /**
     * Зафиксировать таблицы
     */
    public void beforeFlush() {
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

        synchronized (this) {
            curRecordsOffset = recordsOffset;
            curPositionsOffset = positionsOffset;
            recordsOffset += Entries.sizeOfEntry(entry);
            curPositionsOffset += POSITION_LENGTH;
            if (recordsOffset >= currentWriteLog.size()) {
                LOGGER.info("Need to flush");
                tables.add(currentWriteLog);
                currentWriteLog = getNewWriteLog();
                curRecordsOffset = 0;
                curPositionsOffset = 0;
                recordsOffset = Entries.sizeOfEntry(entry);
                positionsOffset = POSITION_LENGTH;
            }
        }
        currentWriteLog.putEntry(curRecordsOffset, entry);
        currentWriteLog.putPositionChecksum(curPositionsOffset, curRecordsOffset);
        currentWriteLog.putPosition(curPositionsOffset + Long.BYTES, curRecordsOffset);
        currentWriteLog.flush();
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
            this.positionsFile =
                    MemorySegments.map(positionsName, Files.size(positionsName), FileChannel.MapMode.READ_ONLY, scope);
            this.recordsFile =
                    MemorySegments.map(tableName, Files.size(tableName), FileChannel.MapMode.READ_ONLY, scope);
        }

        public boolean isEmpty() {
            return positionsFile.byteSize() - positionsOffset < WAL.POSITION_LENGTH;
        }

        public Entry<MemorySegment> peek() {
            if (isEmpty()) {
                return null;
            }
            while (true) {
                long positionChecksum = MemoryAccess.getLongAtOffset(positionsFile, positionsOffset);
                long position = MemoryAccess.getLongAtOffset(positionsFile, positionsOffset + Long.BYTES);
                positionsOffset += POSITION_LENGTH;
                if (positionChecksum != position) {
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
                        .collect(Collectors.toList());
            }
        }
    }

    private static class WriteOnlyLog {
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
            long maxOffsetsLength = (length / (Entries.MIN_LENGTH + Long.BYTES)) * Long.BYTES;
            this.positionsFile =
                    MemorySegments.map(positionsName, maxOffsetsLength, FileChannel.MapMode.READ_WRITE, scope);
        }

        public void putEntry(long offset, Entry<MemorySegment> entry) {
            MemorySegments.writeEntry(recordsFile, offset, entry);
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
