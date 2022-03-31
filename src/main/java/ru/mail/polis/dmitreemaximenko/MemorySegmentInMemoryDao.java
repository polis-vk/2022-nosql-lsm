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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemorySegmentInMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<MemorySegment> comparator = new NaturalOrderComparator();
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data =
            new ConcurrentSkipListMap<>(comparator);
    private static final String LOG_NAME = "log";
    private static final MemorySegment VERY_FIRST_KEY = MemorySegment.ofArray(new byte[]{});
    private static final long NULL_VALUE_SIZE = -1;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Config config;
    private final List<MemorySegment> logs;
    private final ResourceScope scope = ResourceScope.newSharedScope();

    public MemorySegmentInMemoryDao() throws IOException {
        this(null);
    }

    public MemorySegmentInMemoryDao(Config config) throws IOException {
        this.config = config;
        if (config == null) {
            logs = null;
        } else {
            List<Path> logPaths = getLogPaths();
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
        Integer logIndex = 0;
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

    private Path getLogName() {
        Integer logIndex = 0;
        while (true) {
            Path filename = config.basePath().resolve(LOG_NAME + logIndex);
            if (Files.exists(filename)) {
                logIndex++;
            } else {
                break;
            }
        }
        return config.basePath().resolve(LOG_NAME + logIndex);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) throws IOException {
        Iterator<Entry<MemorySegment>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();
        if (comparator.compare(next.key(), key) == 0) {
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

        scope.close();
        lock.writeLock().lock();

        try (ResourceScope writeScope = ResourceScope.newConfinedScope()) {
            // values amount
            long size = Long.BYTES;

            for (Entry<MemorySegment> value : data.values()) {
                if (value.value() == null) {
                    size += value.key().byteSize();
                } else {
                    size += value.value().byteSize() + value.key().byteSize();
                }

                // index, key size, value size
                size += 3L * Long.BYTES;
            }

            Path newLogFile = getLogName();
            Files.createFile(newLogFile);
            MemorySegment log =
                    MemorySegment.mapFile(
                            newLogFile,
                            0,
                            size,
                            FileChannel.MapMode.READ_WRITE,
                            writeScope);

            MemoryAccess.setLongAtOffset(log, 0, data.size());
            long indexOffset = Long.BYTES;
            long dataOffset = Long.BYTES + data.size() * Long.BYTES;

            for (Entry<MemorySegment> value : data.values()) {
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
        } finally {
            lock.writeLock().unlock();
        }
    }
}
