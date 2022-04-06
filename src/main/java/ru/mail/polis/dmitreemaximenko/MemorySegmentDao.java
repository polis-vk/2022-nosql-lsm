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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<MemorySegment> COMPARATOR = NaturalOrderComparator.getInstance();
    private static final String LOG_NAME = "log";
    private static final MemorySegment VERY_FIRST_KEY = MemorySegment.ofArray(new byte[]{});
    private static final long NULL_VALUE_SIZE = -1;
    private static final String TMP_SUFFIX = "tmp";
    private static final int LOG_INDEX_START = 0;
    private int logIndexNextFileName;
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data =
            new ConcurrentSkipListMap<>(COMPARATOR);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Config config;
    private final List<MemorySegment> logs;
    private final ResourceScope scope = ResourceScope.globalScope();

    public MemorySegmentDao() throws IOException {
        this(null);
    }

    public MemorySegmentDao(Config config) throws IOException {
        this.config = config;
        if (config == null) {
            logs = null;
        } else {
            List<Path> logPaths = getLogPaths();
            logIndexNextFileName = logPaths.size();
            logs = new ArrayList<>(logPaths.size());

            for (Path logPath : logPaths) {
                MemorySegment log;
                try {
                    long size = Files.size(logPath);
                    log = MemorySegment.mapFile(logPath, 0, size, FileChannel.MapMode.READ_ONLY, scope);
                } catch (NoSuchFileException e) {
                    log = null;
                }
                logs.add(log);
            }
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) throws IOException {
        lock.readLock().lock();
        MemorySegment fromValue = from;
        try {
            if (from == null) {
                fromValue = VERY_FIRST_KEY;
            }

            if (to == null) {
                return new BorderedIterator(fromValue, null, data.tailMap(fromValue).values().iterator(), logs);
            }
            return new BorderedIterator(fromValue, to, data.subMap(fromValue, to).values().iterator(), logs);
        } finally {
            lock.readLock().unlock();
        }
    }

    private List<Path> getLogPaths() {
        List<Path> result = new LinkedList<>();
        int logIndex = LOG_INDEX_START;
        while (true) {
            Path filename = config.basePath().resolve(LOG_NAME + logIndex);
            if (Files.exists(filename)) {
                logIndex++;
                result.add(filename);
            } else {
                break;
            }
        }

        return result;
    }

    private void removeLogFilesExceptFirst() throws IOException {
        int logIndex = LOG_INDEX_START + 1;
        while (true) {
            Path filename = config.basePath().resolve(LOG_NAME + logIndex);
            if (Files.exists(filename)) {
                logIndex++;
                Files.delete(filename);
            } else {
                break;
            }
        }
    }

    private Path getLogName() {
        return config.basePath().resolve(LOG_NAME + logIndexNextFileName);
    }

    private Path getTmpLogFileName() {
        return config.basePath().resolve(LOG_NAME + TMP_SUFFIX);
    }

    private Path getFirstLogFileName() {
        return config.basePath().resolve(LOG_NAME + LOG_INDEX_START);
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
        lock.readLock().lock();
        try {
            data.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    // values_amount index1 index2 ... indexN k1_size v1_size k1 v1 ....
    @Override
    public void close() throws IOException {
        if (!scope.isAlive()) {
            return;
        }
//        scope.close();
        lock.writeLock().lock();
        try {
            if (!data.isEmpty()) {
                writeValuesToFile(data.values().iterator(), data.values().iterator(), getLogName());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void writeValuesToFile(Iterator<Entry<MemorySegment>> valuesIterator,
                                   Iterator<Entry<MemorySegment>> valuesIteratorCopy,
                                   Path fileName)
            throws IOException {
        try (ResourceScope writeScope = ResourceScope.newConfinedScope()) {
            long size = Long.BYTES;
            long valuesAmount = 0;

            while (valuesIterator.hasNext()) {
                valuesAmount++;
                Entry<MemorySegment> value = valuesIterator.next();
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

            while (valuesIteratorCopy.hasNext()) {
                Entry<MemorySegment> value = valuesIteratorCopy.next();
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
        lock.writeLock().lock();
        try {
            Path tmpLogFileName = getTmpLogFileName();

            // we guarantee here correct behaviour even if crash happens at any time
            // suppose we have logs: L1, L2 and M (memory values)
            // 1) first we right compact log into: L_TMP
            // 2) then we atomically change L1 with L_TMP
            // 3) then we delete L2

            // if we crashed after step 1 - nothing happens, because we will ignore L_TMP and lose only M.
            // if we crashed after step 2 - values that was only in L1 - still in L1 (which is compact), values that
            // were in L1 and L2 we will use still from L2, values that was in memory now in L1,
            // values that was both in memory and L1 or L2 now in L1
            writeValuesToFile(get(null, null), get(null, null), tmpLogFileName);
            Files.move(tmpLogFileName, getFirstLogFileName(), StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            removeLogFilesExceptFirst();
            logIndexNextFileName = 1;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
