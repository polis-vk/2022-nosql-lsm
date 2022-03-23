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
    private final static Comparator<MemorySegment> comparator = new NaturalOrderComparator();
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data =
            new ConcurrentSkipListMap<>(comparator);
    private static final String LOG_NAME = "log";
    private static final MemorySegment VERY_FIRST_KEY = MemorySegment.ofArray(new byte[]{});
    private static final long NULL_VALUE_SIZE = -1;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Config config;
    private final List<MemorySegment> readPages;
    private final ResourceScope scope = ResourceScope.newSharedScope();

    public MemorySegmentInMemoryDao() throws IOException {
        this(null);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) throws IOException {
        lock.readLock().lock();
        try {
            if (from == null) {
                from = VERY_FIRST_KEY;
            }

            if (to == null) {
                return new BorderedIterator(from, null, data.tailMap(from).values().iterator(), readPages);
            }

            return new BorderedIterator(from, to, data.subMap(from, to).values().iterator(), readPages);
        } finally {
            lock.readLock().unlock();
        }
    }

    public MemorySegmentInMemoryDao(Config config) throws IOException {
        this.config = config;
        List<Path> logPaths = getLogPaths();
        readPages = new ArrayList<>(logPaths.size());

        for (Path logPath : logPaths) {
            MemorySegment page;
            try {
                long size = Files.size(logPath);
                page = MemorySegment.mapFile(logPath, 0, size, FileChannel.MapMode.READ_ONLY, scope);
            } catch (NoSuchFileException e) {
                page = null;
            }
            readPages.add(page);
        };
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
                if (entry.value() == null) {
                    return null;
                }
                return entry;
            }

            for (int i = readPages.size() - 1; i >= 0; i--) {
                MemorySegment readPage = readPages.get(i);
                long offset = 0;

                while (offset < readPage.byteSize()) {
                    long keySize = MemoryAccess.getLongAtOffset(readPage, offset);
                    offset += Long.BYTES;
                    long valueSize = MemoryAccess.getLongAtOffset(readPage, offset);
                    offset += Long.BYTES;

                    if (keySize != key.byteSize()) {
                        if (valueSize == NULL_VALUE_SIZE) {
                            valueSize = 0;
                        }
                        offset += keySize + valueSize;
                        continue;
                    }

                    MemorySegment currentKey = readPage.asSlice(offset, keySize);
                    if (key.mismatch(currentKey) == -1) {
                        if (valueSize != NULL_VALUE_SIZE) {
                            return new BaseEntry<>(key, readPage.asSlice(offset + keySize, valueSize));
                        } else {
                            return null;
                        }
                    }
                    offset += keySize + valueSize;
                }
            }

            return null;
        } finally {
            lock.readLock().unlock();
        }
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

    static class NaturalOrderComparator implements Comparator<MemorySegment> {
        @Override
        public int compare(MemorySegment e1, MemorySegment e2) {
            long firstMismatch = e1.mismatch(e2);
            if (firstMismatch == -1) {
                return 0;
            }
            if (firstMismatch == e1.byteSize()) {
                return -1;
            }
            if (firstMismatch == e2.byteSize()) {
                return 1;
            }
            return Byte.compareUnsigned(
                    MemoryAccess.getByteAtOffset(e1, firstMismatch),
                    MemoryAccess.getByteAtOffset(e2, firstMismatch)
            );
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
            MemorySegment page =
                    MemorySegment.mapFile(
                            newLogFile,
                            0,
                            size,
                            FileChannel.MapMode.READ_WRITE,
                            writeScope);

            long offset = 0;
            for (Entry<MemorySegment> value : data.values()) {
                MemoryAccess.setLongAtOffset(page, offset, value.key().byteSize());
                offset += Long.BYTES;
                if (value.value() == null) {
                    MemoryAccess.setLongAtOffset(page, offset, NULL_VALUE_SIZE);
                } else {
                    MemoryAccess.setLongAtOffset(page, offset, value.value().byteSize());
                }
                offset += Long.BYTES;

                page.asSlice(offset).copyFrom(value.key());
                offset += value.key().byteSize();

                if (value.value() != null) {
                    page.asSlice(offset).copyFrom(value.value());
                    offset += value.value().byteSize();
                }
            }

        } finally {
            lock.writeLock().unlock();
        }
    }

    static class BorderedIterator implements Iterator<Entry<MemorySegment>> {
        private final List<Source> sources;

        static class Source {
            Iterator<Entry<MemorySegment>> iterator;
            Entry<MemorySegment> element;

            public Source(Iterator<Entry<MemorySegment>> iterator, Entry<MemorySegment> element) {
                this.iterator = iterator;
                this.element = element;
            }
        }

        private BorderedIterator(MemorySegment from, MemorySegment last, Iterator<Entry<MemorySegment>> iterator,
                                 List<MemorySegment> readPages) {
            sources = new LinkedList<>();
            if (iterator.hasNext()) {
                sources.add(new Source(iterator, iterator.next()));
            }

            for (int i = readPages.size() - 1; i >= 0; i--) {
                Iterator<Entry<MemorySegment>> fileIterator = new FileEntryIterator(from, last, readPages.get(i));
                if (fileIterator.hasNext()) {
                    sources.add(new Source(fileIterator, fileIterator.next()));
                }
            }

            removeNextNullValues();
        }

        @Override
        public boolean hasNext() {
            return !sources.isEmpty();
        }

        @Override
        public Entry<MemorySegment> next() {
            Source source = peekIterator();
            Entry<MemorySegment> result = source.element;
            moveAllIteratorsWithSuchKey(result.key());
            removeNextNullValues();
            return result;
        }

        private void removeNextNullValues() {
            Source source = peekIterator();
            while (source != null && source.element.value() == null) {
                moveAllIteratorsWithSuchKey(source.element.key());
                source = peekIterator();
            }
        }

        private Source peekIterator() {
            if (sources.isEmpty()) {
                return null;
            }

            MemorySegment minKey = sources.get(0).element.key();
            Source minSource = sources.get(0);
            for (Source source : sources) {
                if (comparator.compare(source.element.key(), minKey) < 0) {
                    minKey = source.element.key();
                    minSource = source;
                }
            }

            return minSource;
        }

        private void moveAllIteratorsWithSuchKey(MemorySegment key) {
            List<Source> toRemove = new LinkedList<>();
            for (Source source : sources) {
                if (comparator.compare(source.element.key(), key) == 0) {
                    if (source.iterator.hasNext()) {
                        source.element = source.iterator.next();
                    } else {
                        toRemove.add(source);
                    }
                }
            }
            sources.removeAll(toRemove);
        }

        static class FileEntryIterator implements Iterator<Entry<MemorySegment>> {
            private long offset;
            private MemorySegment log;
            private final MemorySegment last;
            private BaseEntry<MemorySegment> next = null;

            private FileEntryIterator(MemorySegment from, MemorySegment last, MemorySegment log) {
                offset = 0;
                this.log = log;
                while (offset < log.byteSize()) {
                    long keySize = MemoryAccess.getLongAtOffset(log, offset);
                    offset += Long.BYTES;
                    long valueSize = MemoryAccess.getLongAtOffset(log, offset);
                    offset += Long.BYTES;

                    MemorySegment currentKey = log.asSlice(offset, keySize);
                    if (comparator.compare(from, currentKey) > 0) {
                        if (valueSize == NULL_VALUE_SIZE) {
                            valueSize = 0;
                        }
                        offset += keySize + valueSize;
                    } else {
                        if (valueSize != NULL_VALUE_SIZE) {
                            next = new BaseEntry<>(currentKey, log.asSlice(offset + keySize, valueSize));
                        } else {
                            next = new BaseEntry<>(currentKey, null);
                        }
                        if (valueSize == NULL_VALUE_SIZE) {
                            valueSize = 0;
                        }
                        offset += keySize + valueSize;
                        break;
                    }

                }

                this.last = last == null ? null : MemorySegment.ofArray(last.toByteArray());
            }

            @Override
            public boolean hasNext() {
                return next != null && (last == null || comparator.compare(next.key(), last) < 0);
            }

            @Override
            public Entry<MemorySegment> next() {
                Entry<MemorySegment> result = next;
                next = null;

                if (offset < log.byteSize()) {
                    long keySize = MemoryAccess.getLongAtOffset(log, offset);
                    offset += Long.BYTES;
                    long valueSize = MemoryAccess.getLongAtOffset(log, offset);
                    offset += Long.BYTES;

                    MemorySegment currentKey = log.asSlice(offset, keySize);
                    if (valueSize != NULL_VALUE_SIZE) {
                        next = new BaseEntry<>(currentKey, log.asSlice(offset + keySize, valueSize));
                    } else {
                        next = new BaseEntry<>(currentKey, null);
                    }
                    if (valueSize == NULL_VALUE_SIZE) {
                        valueSize = 0;
                    }
                    offset += keySize + valueSize;
                }
                return result;
            }
        }
    }
}
