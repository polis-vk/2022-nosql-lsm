package ru.mail.polis.dmitreemaximenko;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;
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

    public MemorySegmentInMemoryDao(Config config) throws IOException {
        this.config = config;
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
        lock.readLock().lock();
        try {
            Entry<MemorySegment> entry = data.get(key);
            if (entry != null) {
                return entry.value() == null ? null : entry;
            }
            return checkLogsForKey(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    private Entry<MemorySegment> checkLogsForKey(MemorySegment key) {
        for (int i = logs.size() - 1; i >= 0; i--) {
            MemorySegment log = logs.get(i);
            long offset = 0;

            while (offset < log.byteSize()) {
                long keySize = MemoryAccess.getLongAtOffset(log, offset);
                offset += Long.BYTES;
                long valueSize = MemoryAccess.getLongAtOffset(log, offset);
                offset += Long.BYTES;

                if (keySize != key.byteSize()) {
                    offset += keySize;
                    offset = valueSize == NULL_VALUE_SIZE ? offset : offset + valueSize;
                    continue;
                }

                MemorySegment currentKey = log.asSlice(offset, keySize);
                if (key.mismatch(currentKey) == -1) {
                    return valueSize == NULL_VALUE_SIZE ? null :
                            new BaseEntry<>(key, log.asSlice(offset + keySize, valueSize));
                }
                offset += keySize + valueSize;
            }
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

    @Override
    public void close() throws IOException {
        if (!scope.isAlive()) {
            return;
        }
        scope.close();

        lock.writeLock().lock();
        try (ResourceScope writeScope = ResourceScope.newConfinedScope()) {
            long size = 0;
            for (Entry<MemorySegment> value : data.values()) {
                if (value.value() == null) {
                    size += value.key().byteSize();
                } else {
                    size += value.value().byteSize() + value.key().byteSize();
                }
            }
            size += 2L * data.size() * Long.BYTES;
            Path newLogFile = getLogName();
            Files.createFile(newLogFile);
            MemorySegment log =
                    MemorySegment.mapFile(
                            newLogFile,
                            0,
                            size,
                            FileChannel.MapMode.READ_WRITE,
                            writeScope);
            long offset = 0;
            for (Entry<MemorySegment> value : data.values()) {
                MemoryAccess.setLongAtOffset(log, offset, value.key().byteSize());
                offset += Long.BYTES;
                if (value.value() == null) {
                    MemoryAccess.setLongAtOffset(log, offset, NULL_VALUE_SIZE);
                } else {
                    MemoryAccess.setLongAtOffset(log, offset, value.value().byteSize());
                }
                offset += Long.BYTES;

                log.asSlice(offset).copyFrom(value.key());
                offset += value.key().byteSize();
                if (value.value() != null) {
                    log.asSlice(offset).copyFrom(value.value());
                    offset += value.value().byteSize();
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
