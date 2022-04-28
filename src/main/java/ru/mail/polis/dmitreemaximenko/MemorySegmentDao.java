package ru.mail.polis.dmitreemaximenko;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<MemorySegment> COMPARATOR = NaturalOrderComparator.getInstance();
    private static final String LOG_NAME = "log";
    private static final MemorySegment VERY_FIRST_KEY = MemorySegment.ofArray(new byte[]{});
    private static final long NULL_VALUE_SIZE = -1;
    private static final int FLUSHING_QUEUE_SIZE = 2;
    private static final String TMP_COMPACT_FILE = "compact_log_tmp";
    private static final String TMP_FLUSH_FILE = "flush_log_tmp";
    private static final String COMPACTING_FILE = "compacting_tmp";
    private static final long DEFAULT_TABLE_SPACE = 0124 * 1024 * 128;
    private static int SUCCESS = 0;
    private static int FLUSH_REQUEST = -1;
    private static int TABLE_READ_ONLY = -2;
    private static final int LOG_INDEX_START = 0;
    private volatile boolean flushFinish = false;
    private int logIndexNextFileName;
    private MemTable activeMemTable;
    private BlockingQueue<MemTable> flushingTables = new ArrayBlockingQueue<MemTable>(FLUSHING_QUEUE_SIZE);
    private List<SSTable> ssTables;
    private Thread flusher;
    private ExecutorService compactor;
    private volatile MemTable flushingTable;
    private final ReadWriteLock DBTablesLock = new ReentrantReadWriteLock();
    private final ReadWriteLock logDirectoryLock = new ReentrantReadWriteLock();
    private final ReadWriteLock flushLock = new ReentrantReadWriteLock();
    private final Config config;
    private final ResourceScope scope = ResourceScope.globalScope();

    public MemorySegmentDao() throws IOException {
        this(null);
    }

    public MemorySegmentDao(Config config) throws IOException {
        this.config = config;
        this.compactor = Executors.newSingleThreadExecutor();

        if (Files.exists(getCompactTmpFile())) {
            finishCompacting();
        }

        flusher = new Thread(() -> {
            while (true) {
                flushLock.readLock().lock();
                if (flushFinish) {
                    break;
                }
                flushLock.readLock().unlock();
                try {
                    try {
                        MemTable tableToFlush = flushingTables.take();

                        DBTablesLock.writeLock().lock();
                        flushingTable = tableToFlush;
                        DBTablesLock.writeLock().unlock();

                        if (!flushingTable.isEmpty()) {
                            writeValuesToFile(flushingTable, getFlushTmpFile());

                            logDirectoryLock.writeLock().lock();

                            int newLogIndex = logIndexNextFileName;
                            try {
                                Files.move(getFlushTmpFile(),
                                        getLogName(logIndexNextFileName));
                                logIndexNextFileName++;
                            } finally {
                                logDirectoryLock.writeLock().unlock();
                            }


                            DBTablesLock.writeLock().lock();
                            ssTables.add(0, new SSTable(getLogName(newLogIndex), scope));
                            DBTablesLock.writeLock().unlock();
                        }

                    } catch (InterruptedException e) {
                        while (!flushingTables.isEmpty()) {
                            flushingTable = flushingTables.poll();
                            if (!flushingTable.isEmpty()) {
                                writeValuesToFile(flushingTable, getLogName());
                            }
                        }
                        break;
                    }
                } catch (IOException ioException)
                {
                    ioException.printStackTrace();
                }
            }

            while (!flushingTables.isEmpty()) {
                MemTable tableToFlush = flushingTables.poll();

                DBTablesLock.writeLock().lock();
                flushingTable = tableToFlush;
                Path filename = getLogName();
                DBTablesLock.writeLock().unlock();

                if (!flushingTable.isEmpty()) {
                    try {
                        writeValuesToFile(flushingTable, filename);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        flusher.start();
        activeMemTable = createMemoryTable();
        if (config == null) {
            ssTables = null;
        } else {
            List<Path> logPaths = getLogPaths();
            logIndexNextFileName = logPaths.size();
            ssTables = new ArrayList<>(logPaths.size());

            for (Path logPath : logPaths) {
                ssTables.add(0, new SSTable(logPath, scope));
            }
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) throws IOException {

        MemorySegment fromValue = from;

        if (from == null) {
            fromValue = VERY_FIRST_KEY;
        }

        if (to == null) {
            return new BorderedMergeIterator(fromValue, null, getAvailableTables());
        }
        return new BorderedMergeIterator(fromValue, to, getAvailableTables());

    }

    // should get them in correct order. Less is more recent
    private List<Table> getAvailableTables() {
        List<Table> result = new LinkedList<>();
        DBTablesLock.readLock().lock();
        try {
            result.add(activeMemTable);
            result.addAll(flushingTables);
            if (flushingTable != null) {
                result.add(flushingTable);
            }
            result.addAll(ssTables);
        } finally {
            DBTablesLock.readLock().unlock();
        }

        return result;
    }

    private List<Path> getLogPaths() {
        List<Path> result = new LinkedList<>();
        int logIndex = LOG_INDEX_START;
        logDirectoryLock.readLock().lock();
        try {
            while (true) {
                Path filename = config.basePath().resolve(LOG_NAME + logIndex);
                if (Files.exists(filename)) {
                    logIndex++;
                    result.add(filename);
                } else {
                    break;
                }
            }
        } finally {
            logDirectoryLock.readLock().unlock();
        }

        return result;
    }

    private void removeLogFilesWithoutLocking() throws IOException {
        for (int logIndex = logIndexNextFileName - 1; logIndex > 0; logIndex--) {
            Path filename = config.basePath().resolve(LOG_NAME + logIndex);
            Files.delete(filename);
        }
    }

    private Path getLogNameWithoutLocking() {
        return config.basePath().resolve(LOG_NAME + logIndexNextFileName);
    }

    private Path getLogName() {
        logDirectoryLock.readLock().lock();
        Path result = config.basePath().resolve(LOG_NAME + logIndexNextFileName);
        logDirectoryLock.readLock().unlock();
        return result;
    }

    private Path getLogNameWithoutLocking(int logNumber) {
        return config.basePath().resolve(LOG_NAME + logNumber);
    }

    private Path getLogName(int logNumber) {
        return config.basePath().resolve(LOG_NAME + logNumber);
    }

    private Path getFirstLogFileName() {
        return config.basePath().resolve(LOG_NAME + LOG_INDEX_START);
    }

    private Path getCompactTmpFile() {
        return config.basePath().resolve(TMP_COMPACT_FILE);
    }

    private Path getFlushTmpFile() {
        return config.basePath().resolve(TMP_FLUSH_FILE);
    }

    private Path getCompactingInProcessFile() {return config.basePath().resolve(COMPACTING_FILE);}

    @Override
    public Entry<MemorySegment> get(MemorySegment key) throws IOException {
        Iterator<Entry<MemorySegment>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();
        if (COMPARATOR.compare(next.key(), key) == 0) {
            return next;
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        while (true) {
            int insertResult = activeMemTable.put(entry.key(), entry);
            if (insertResult == SUCCESS) {
                break;
            }
            if (insertResult == TABLE_READ_ONLY) {
                // #fixMe polling
                continue;
            }

            assert insertResult == FLUSH_REQUEST;
            DBTablesLock.writeLock().lock();
            try {
                if (flushingTables.size() < FLUSHING_QUEUE_SIZE) {
                    flushWithoutLocking();
                } else {
                    throw new OutOfMemoryError();
                }
            } finally {
                DBTablesLock.writeLock().unlock();
            }
        }
    }

    public void flushWithoutLocking() {
        flushingTables.add(activeMemTable);
        activeMemTable = createMemoryTable();
    }

    @Override
    public void flush() {
        DBTablesLock.writeLock().lock();

        try {
            flushWithoutLocking();
        } finally {
            DBTablesLock.writeLock().unlock();
        }
    }

    private void finishCompacting() throws IOException {
        List<Path> logPaths = getLogPaths();
        logIndexNextFileName = logPaths.size();
        removeLogFilesWithoutLocking();
        Files.move(getCompactTmpFile(), getFirstLogFileName(), StandardCopyOption.ATOMIC_MOVE);
        logIndexNextFileName = 1;
    }

    // values_amount index1 index2 ... indexN k1_size v1_size k1 v1 ....
    @Override
    public void close() throws IOException {
        if (!scope.isAlive()) {
            return;
        }
        flushLock.writeLock().lock();
        flushFinish = true;
        flush();
        flushLock.writeLock().unlock();

        try {
            flusher.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        compactor.shutdown();
    }

    private MemTable createMemoryTable() {
        if (config == null) {
            return new MemTable(DEFAULT_TABLE_SPACE);
        }

        return new MemTable(config.flushThresholdBytes());
    }

    private void writeValuesToFile(Iterable<Entry<MemorySegment>> values,
                                   Path fileName)
            throws IOException {
        try (ResourceScope writeScope = ResourceScope.newConfinedScope()) {
            long size = Long.BYTES;
            long valuesAmount = 0;

            for (Entry<MemorySegment> value : values) {
                valuesAmount++;
                if (value.value() == null) {
                    size += value.key().byteSize();
                } else {
                    size += value.value().byteSize() + value.key().byteSize();
                }

                // index, key size, value size
                size += 3L * Long.BYTES;
            }

            if (Files.exists(fileName)) {
                Files.delete(fileName);
            }
            Files.createFile(fileName);
            MemorySegment log =
                    MemorySegment.mapFile(
                            fileName,
                            0,
                            size,
                            FileChannel.MapMode.READ_WRITE,
                            writeScope);

            MemoryAccess.setLongAtOffset(log, 0, valuesAmount);
            long indexOffset = Long.BYTES;
            long dataOffset = Long.BYTES + valuesAmount * Long.BYTES;

            for (Entry<MemorySegment> value : values) {
                MemoryAccess.setLongAtOffset(log, indexOffset, dataOffset);
                indexOffset += Long.BYTES;

                MemoryAccess.setLongAtOffset(log, dataOffset, value.key().byteSize());
                dataOffset += Long.BYTES;
                if (value.value() == null) {
                    MemoryAccess.setLongAtOffset(log, dataOffset, NULL_VALUE_SIZE);
                } else {
                    MemoryAccess.setLongAtOffset(log, dataOffset, value.value().byteSize());
                }

                dataOffset += Long.BYTES;

                log.asSlice(dataOffset).copyFrom(value.key());
                dataOffset += value.key().byteSize();
                if (value.value() != null) {
                    log.asSlice(dataOffset).copyFrom(value.value());
                    dataOffset += value.value().byteSize();
                }
            }
        }
    }

    @Override
    public void compact() throws IOException {
        compactor.execute(() -> {
            try {
                List<Table> ssTablesToCompact = new ArrayList<>(ssTables.size());

                DBTablesLock.readLock().lock();
                if (ssTables.size() <= 1) {
                    DBTablesLock.readLock().unlock();
                    return;
                }
                ssTablesToCompact.addAll(ssTables);
                DBTablesLock.readLock().unlock();

                Files.deleteIfExists(getCompactingInProcessFile());
                writeValuesToFile(() -> {
                    try {
                        return new BorderedMergeIterator(ssTablesToCompact);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return Collections.emptyIterator();
                }, getCompactingInProcessFile());

                logDirectoryLock.writeLock().lock();
                try {
                    Files.move(getCompactingInProcessFile(), getCompactTmpFile(), StandardCopyOption.ATOMIC_MOVE);
                    removeLogFilesWithoutLocking();
                    Files.move(getCompactTmpFile(), getFirstLogFileName(), StandardCopyOption.ATOMIC_MOVE);
                    logIndexNextFileName = 1;

                    // fixing flushed during compaction files
                    int logNumber = ssTablesToCompact.size();
                    // can use getLogNameWithoutLocking() because already hold logDirectoryLock.writeLock() lock
                    while (Files.exists(getLogNameWithoutLocking(logNumber))) {
                        Files.move(getLogNameWithoutLocking(logNumber), getLogNameWithoutLocking(logIndexNextFileName));
                        logIndexNextFileName++;
                    }
                } finally {
                    logDirectoryLock.writeLock().unlock();
                }

                DBTablesLock.writeLock().lock();
                try {
                    ssTables.clear();
                    ssTables.add(new SSTable(getFirstLogFileName(), scope));
                } finally {
                    DBTablesLock.writeLock().unlock();
                }
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        });
    }
}
