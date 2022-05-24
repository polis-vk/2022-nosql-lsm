package ru.mail.polis.vladislavfetisov.wal;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Config;
import ru.mail.polis.Entry;
import ru.mail.polis.vladislavfetisov.Entries;
import ru.mail.polis.vladislavfetisov.MemorySegments;
import ru.mail.polis.vladislavfetisov.Utils;
import ru.mail.polis.vladislavfetisov.lsm.LsmDao;
import ru.mail.polis.vladislavfetisov.lsm.SSTable;
import ru.mail.polis.vladislavfetisov.lsm.Storage;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WAL {
    private static final String LOG_DIR = "log" + File.pathSeparator;
    private static final byte START_BYTE = -1;
    private final Path dir;
    private final Config config;
    private final ResourceScope writeScope = ResourceScope.newSharedScope();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private volatile long offset = 0;
    private volatile List<WriteOnlyLog> tables;
    private volatile List<WriteOnlyLog> tablesForDelete;
    private volatile WriteOnlyLog currentWriteLog;
    private long tableNum = 0;

    public WAL(Config config) throws IOException {
        this.config = config;
        this.dir = config.basePath().resolve(LOG_DIR);
        this.currentWriteLog = getNewWriteLog();
        this.tables = getNewTables();
        this.tablesForDelete = Collections.emptyList();
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

    public LsmDao.State restoreFromDir(LsmDao.State state) throws IOException {
        try (ResourceScope scopeForRead = ResourceScope.newSharedScope()) {
            List<ReadOnlyLog> logTables = ReadOnlyLog.getReadOnlyLogs(dir, scopeForRead);
            for (ReadOnlyLog logTable : logTables) {
                while (true) {
                    Storage storage = state.storage();
                    boolean oversize = false;
                    while (!logTable.isEmpty()) {
                        Entry<MemorySegment> entry = logTable.peek();
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
        }
        this.tablesForDelete = Collections.emptyList();
    }

    public void put(Entry<MemorySegment> entry) throws IOException {
        long currentOffset;
        synchronized (this) {
            currentOffset = offset;
            offset += Byte.SIZE + Entries.sizeOfEntry(entry);
            if (offset >= currentWriteLog.size()) {
                tables.add(currentWriteLog);
                currentWriteLog = getNewWriteLog();
                currentOffset = 0;
                offset = Byte.SIZE + Entries.sizeOfEntry(entry);
            }
        }
        rwLock.readLock().lock();
        try {
            currentWriteLog.putByte(currentOffset, START_BYTE);
            currentWriteLog.putEntry(currentOffset + Byte.SIZE, entry);
        } finally {
            rwLock.writeLock().unlock();
        }

        rwLock.writeLock().lock();
        try {
            currentWriteLog.flush();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Not thread safe
     */
    private static class ReadOnlyLog {
        private final MemorySegment mapFile;
        private final Path tableName;
        private long offset = 0;

        private ReadOnlyLog(Path tableName, ResourceScope scope) throws IOException {
            this.tableName = tableName;
            this.mapFile = MemorySegments.map(tableName, Files.size(tableName), FileChannel.MapMode.READ_ONLY, scope);
        }

        public boolean isEmpty() {
            return offset == mapFile.byteSize();
        }

        public Entry<MemorySegment> peek() {
            if (mapFile.byteSize() - offset - 1 < Entries.MIN_LENGTH) {//FIXME BAD
                offset = mapFile.byteSize();
                return null;
            }
            Entry<MemorySegment> entry = MemorySegments.readEntryByOffset(mapFile, offset);
            offset += Entries.sizeOfEntry(entry);

            return entry;
        }

        public static List<ReadOnlyLog> getReadOnlyLogs(Path dir, ResourceScope scope) throws IOException {
            try (Stream<Path> files = Files.list(dir)) {
                return files.mapToInt(Utils::getTableNum)
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
        private final MemorySegment mapFile;
        private final Path tableName;

        public long size() {
            return mapFile.byteSize();
        }

        public WriteOnlyLog(Path tableName, ResourceScope scope, long length) throws IOException {
            this.tableName = tableName;
            Utils.newFile(tableName);
            this.mapFile = MemorySegments.map(tableName, length, FileChannel.MapMode.READ_WRITE, scope);
        }

        public void putByte(long offset, byte value) {
            MemoryAccess.setByteAtOffset(mapFile, offset, value);
        }

        public void putEntry(long offset, Entry<MemorySegment> entry) {
            MemorySegments.writeEntry(mapFile, offset, entry);
        }

        public void flush() {
            mapFile.force();
        }
    }

}
