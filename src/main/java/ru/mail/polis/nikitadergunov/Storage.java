package ru.mail.polis.nikitadergunov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

final class Storage implements Closeable {

    public static volatile Thread flushingThread;
    public static volatile Thread compactThread;
    public static volatile boolean isCompacted;
    public static final ReentrantLock mutex = new ReentrantLock();
    private static final long VERSION = 0;
    private static final int INDEX_HEADER_SIZE = Long.BYTES * 2;
    private static final int INDEX_RECORD_SIZE = Long.BYTES;

    private static final String FILE_NAME = "data";
    private static final String FILE_EXT = ".dat";
    private static final String FILE_EXT_TMP = ".tmp";
    private static final int LOW_PRIORITY_FILE = 0;
    private static final AtomicInteger maxPriorityFile = new AtomicInteger();
    private static final Comparator<Path> fileComparator = Comparator.comparingInt(Storage::getPriorityFile);

    private final ResourceScope scope;
    private final List<MemorySegment> sstables;

    static Storage load(Config config) throws IOException {
        Path basePath = config.basePath();

        List<MemorySegment> sstables = new ArrayList<>();
        ResourceScope scope = ResourceScope.newSharedScope();

        try (Stream<Path> streamFiles = Files.list(basePath)) {
            List<Path> sstablesFiles = streamFiles
                    .filter(path -> path.toString().endsWith(FILE_EXT))
                    .sorted(fileComparator.reversed())
                    .toList();

            if (!sstablesFiles.isEmpty()) {
                maxPriorityFile.set(getPriorityFile(sstablesFiles.get(0)));
            }

            for (Path path : sstablesFiles) {
                sstables.add(mapForRead(scope, path));
            }
        }

        return new Storage(scope, sstables);
    }

    static void flush(Config config,
                      ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memory) {
        Runnable flushRun = () -> {
            mutex.lock();
            try {
                Storage.save(config, memory.values());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                mutex.unlock();
            }
        };
        flushingThread = new Thread(flushRun);
        flushingThread.start();
        isCompacted = false;
        flushingThread = null;
    }

    static void compact(Config config,
                        Storage previousState) {
        if (previousState.sstables.size() < 2 || isCompacted) {
            return;
        }
        Runnable compactRun = () -> {
            mutex.lock();
            try {
                compactTask(config, previousState);
            } finally {
                mutex.unlock();
            }
        };

        compactThread = new Thread(compactRun);
        compactThread.start();
        isCompacted = true;
        compactThread = null;
    }

    private static void compactTask(Config config,
                                    Storage previousState) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(previousState.iterate(null, null));
        Iterator<Entry<MemorySegment>> mergeIterator = MergeIterator.of(iterators, EntryKeyComparator.INSTANCE);
        Iterator<Entry<MemorySegment>> entriesIterator = new MemorySegmentDao.TombstoneFilteringIterator(mergeIterator);
        List<Entry<MemorySegment>> entries = new ArrayList<>();
        while (entriesIterator.hasNext()) {
            entries.add(entriesIterator.next());
        }
        if (entries.isEmpty()) {
            return;
        }
        try {
            Storage.save(config, entries);
            Path sstablePathOld = config.basePath().resolve(FILE_NAME + maxPriorityFile + FILE_EXT);
            Path sstablePathNew = config.basePath().resolve(FILE_NAME + LOW_PRIORITY_FILE + FILE_EXT);

            try (Stream<Path> listFiles = Files.list(config.basePath())) {
                listFiles.filter(path -> !path.equals(sstablePathOld))
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        });

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            Files.move(sstablePathOld, sstablePathNew, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // it is supposed that entries can not be changed externally during this method call
    static void save(
            Config config,
            Collection<Entry<MemorySegment>> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }

        int nextSSTableIndex = maxPriorityFile.incrementAndGet();
        long entriesCount = entries.size();
        long dataStart = INDEX_HEADER_SIZE + INDEX_RECORD_SIZE * entriesCount;

        Path sstableTmpPath = config.basePath().resolve(FILE_NAME + nextSSTableIndex + FILE_EXT_TMP);

        Files.deleteIfExists(sstableTmpPath);
        Files.createFile(sstableTmpPath);

        try (ResourceScope writeScope = ResourceScope.newConfinedScope()) {
            long size = 0;
            for (Entry<MemorySegment> entry : entries) {
                if (entry.value() == null) {
                    size += Long.BYTES + entry.key().byteSize() + Long.BYTES;
                } else {
                    size += Long.BYTES + entry.value().byteSize() + entry.key().byteSize() + Long.BYTES;
                }
            }

            MemorySegment nextSSTable = MemorySegment.mapFile(
                    sstableTmpPath,
                    0,
                    dataStart + size,
                    FileChannel.MapMode.READ_WRITE,
                    writeScope
            );

            long index = 0;
            long offset = dataStart;
            for (Entry<MemorySegment> entry : entries) {
                MemoryAccess.setLongAtOffset(nextSSTable, INDEX_HEADER_SIZE + index * INDEX_RECORD_SIZE, offset);

                offset += writeRecord(nextSSTable, offset, entry.key());
                offset += writeRecord(nextSSTable, offset, entry.value());

                index++;
            }

            MemoryAccess.setLongAtOffset(nextSSTable, 0, VERSION);
            MemoryAccess.setLongAtOffset(nextSSTable, Long.BYTES, entriesCount);

            nextSSTable.force();
        }
        Path sstablePath = config.basePath().resolve(FILE_NAME + nextSSTableIndex + FILE_EXT);
        Files.move(sstableTmpPath, sstablePath, StandardCopyOption.ATOMIC_MOVE);
        maxPriorityFile.incrementAndGet();
    }

    private static long writeRecord(MemorySegment nextSSTable, long offset, MemorySegment record) {
        if (record == null) {
            MemoryAccess.setLongAtOffset(nextSSTable, offset, -1);
            return Long.BYTES;
        }
        long recordSize = record.byteSize();
        MemoryAccess.setLongAtOffset(nextSSTable, offset, recordSize);
        nextSSTable.asSlice(offset + Long.BYTES, recordSize).copyFrom(record);
        return Long.BYTES + recordSize;
    }

    @SuppressWarnings("DuplicateThrows")
    private static MemorySegment mapForRead(ResourceScope scope, Path file) throws IOException {
        long size = Files.size(file);

        return MemorySegment.mapFile(file, 0, size, FileChannel.MapMode.READ_ONLY, scope);
    }

    private static int getPriorityFile(Path path) {
        String file = path.getFileName().toString();
        return Integer.parseInt(file.substring(
                FILE_NAME.length(),
                file.length() - FILE_EXT.length()));
    }

    private Storage(ResourceScope scope, List<MemorySegment> sstables) {
        this.scope = scope;
        this.sstables = sstables;
    }

    private long greaterOrEqualEntryIndex(MemorySegment sstable, MemorySegment key) {
        long index = entryIndex(sstable, key);
        if (index < 0) {
            return ~index;
        }
        return index;
    }

    // file structure:
    // (fileVersion)(entryCount)((entryPosition)...)|((keySize/key/valueSize/value)...)
    private long entryIndex(MemorySegment sstable, MemorySegment key) {
        long fileVersion = MemoryAccess.getLongAtOffset(sstable, 0);
        if (fileVersion != 0) {
            throw new IllegalStateException("Unknown file version: " + fileVersion);
        }
        long recordsCount = MemoryAccess.getLongAtOffset(sstable, 8);
        if (key == null) {
            return recordsCount;
        }

        long left = 0;
        long right = recordsCount - 1;

        while (left <= right) {
            long mid = (left + right) >>> 1;

            long keyPos = MemoryAccess.getLongAtOffset(sstable, INDEX_HEADER_SIZE + mid * INDEX_RECORD_SIZE);
            long keySize = MemoryAccess.getLongAtOffset(sstable, keyPos);

            MemorySegment keyForCheck = sstable.asSlice(keyPos + Long.BYTES, keySize);
            int comparedResult = MemorySegmentComparator.INSTANCE.compare(key, keyForCheck);
            if (comparedResult > 0) {
                left = mid + 1;
            } else if (comparedResult < 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }

        return ~left;
    }

    private Entry<MemorySegment> entryAt(MemorySegment sstable, long keyIndex) {
        long offset = MemoryAccess.getLongAtOffset(sstable, INDEX_HEADER_SIZE + keyIndex * INDEX_RECORD_SIZE);
        long keySize = MemoryAccess.getLongAtOffset(sstable, offset);
        long valueOffset = offset + Long.BYTES + keySize;
        long valueSize = MemoryAccess.getLongAtOffset(sstable, valueOffset);
        return new BaseEntry<>(
                sstable.asSlice(offset + Long.BYTES, keySize),
                valueSize == -1 ? null : sstable.asSlice(valueOffset + Long.BYTES, valueSize)
        );
    }

    private Iterator<Entry<MemorySegment>> iterate(MemorySegment sstable, MemorySegment keyFrom, MemorySegment keyTo) {
        long keyFromPos = greaterOrEqualEntryIndex(sstable, keyFrom);
        long keyToPos = greaterOrEqualEntryIndex(sstable, keyTo);

        return new Iterator<>() {
            long pos = keyFromPos;

            @Override
            public boolean hasNext() {
                return pos < keyToPos;
            }

            @Override
            public Entry<MemorySegment> next() {
                Entry<MemorySegment> entry = entryAt(sstable, pos);
                pos++;
                return entry;
            }
        };
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        long keyFromPos;
        for (MemorySegment sstable : sstables) {
            keyFromPos = entryIndex(sstable, key);
            if (keyFromPos >= 0) {
                return entryAt(sstable, keyFromPos);
            }
        }
        return null;
    }

    public List<Iterator<Entry<MemorySegment>>> iterate(MemorySegment keyFrom, MemorySegment keyTo) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(sstables.size());
        for (MemorySegment sstable : sstables) {
            iterators.add(iterate(sstable, keyFrom, keyTo));
        }
        return iterators;
    }

    @Override
    public synchronized void close() {
        try {
            if (flushingThread != null) {
                flushingThread.join();
            }
            if (compactThread != null) {
                compactThread.join();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (scope.isAlive()) {
            scope.close();
        }
    }

    public boolean isClosed() {
        return !scope.isAlive();
    }

}
