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
    private static final MemorySegment VERY_FIRST_KEY = MemorySegment.ofArray(new byte[]{});
    private static final long NULL_VALUE_SIZE = -1;
    private static final int FLUSHING_QUEUE_SIZE = 1;
    private static final long DEFAULT_TABLE_SPACE = 0124 * 1024 * 128;
    private static int SUCCESS = 0;
    private static int FLUSH_REQUEST = -1;
    private static int TABLE_READ_ONLY = -2;
    private volatile boolean flushFinish = false;
    private MemTable activeMemTable;
    private BlockingQueue<MemTable> flushingTables = new ArrayBlockingQueue<MemTable>(FLUSHING_QUEUE_SIZE);
    private List<SSTable> ssTables;
    private Thread flusher;
    private ExecutorService compactor;
    private volatile MemTable flushingTable;
    private final ReadWriteLock DBTablesLock = new ReentrantReadWriteLock();
    private final ReadWriteLock flushLock = new ReentrantReadWriteLock();
    private final Config config;
    private final ResourceScope scope = ResourceScope.globalScope();
    private final FileManager fileManager;

    public MemorySegmentDao() throws IOException {
        this(null);
    }

    public MemorySegmentDao(Config config) throws IOException {
        this.config = config;
        this.compactor = Executors.newSingleThreadExecutor();

        fileManager = new FileManager(config);

        if (Files.exists(fileManager.getCompactTmpFile())) {
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
                            writeValuesToFile(flushingTable, fileManager.getFlushTmpFile());

                            int newLogIndex = fileManager.addLog(fileManager.getFlushTmpFile());
                            DBTablesLock.writeLock().lock();
                            ssTables.add(0, new SSTable(fileManager.getLogName(newLogIndex), scope));
                            DBTablesLock.writeLock().unlock();
                        }

                    } catch (InterruptedException e) {
                        while (!flushingTables.isEmpty()) {
                            flushingTable = flushingTables.poll();
                            if (!flushingTable.isEmpty()) {
                                writeValuesToFile(flushingTable, fileManager.getNextLogName());
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
                Path filename = fileManager.getNextLogName();
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
            List<Path> logPaths = fileManager.getLogPaths();
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
            if (flushingTables.size() < FLUSHING_QUEUE_SIZE) {
                flushWithoutLocking();
            } else {
                throw new OutOfMemoryError();
            }
        } finally {
            DBTablesLock.writeLock().unlock();
        }
    }

    private void finishCompacting() throws IOException {
        fileManager.addLog(fileManager.getCompactTmpFile());
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

                Files.deleteIfExists(fileManager.getCompactingInProcessFile());
                writeValuesToFile(() -> {
                    try {
                        return new BorderedMergeIterator(ssTablesToCompact);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return Collections.emptyIterator();
                }, fileManager.getCompactingInProcessFile());

                int compactLogIndex;
                fileManager.lock();
                try {
                    Files.move(fileManager.getCompactingInProcessFile(), fileManager.getCompactTmpFile(), StandardCopyOption.ATOMIC_MOVE);
                    fileManager.removeLogFilesWithoutLockingWithFixingFurtherLogs(ssTablesToCompact.size());
                    compactLogIndex = fileManager.addLogWithoutLocking(fileManager.getCompactTmpFile());
                } finally {
                    fileManager.unlock();
                }

                DBTablesLock.writeLock().lock();
                try {
                    ssTables.clear();
                    ssTables.add(new SSTable(fileManager.getLogName(compactLogIndex), scope));
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
