package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class Storage implements Closeable {

    private static final ThreadLocal<Integer> compactedFilesCount = new ThreadLocal<>();

    // File structure
    // [Entries count][Entry1 offset]...[EntryN offset] | [KeySize][Key][ValueSize][Value]
    private static final String DATA_NAME = "data";
    private static final String DATA_EXT = ".dat";

    private static final String COMPACT_DIR = "compact";
    private static final String COMPACT = "ready_to_read";

    // File to save one number - count of compacted - to safe delete after crushing
    private static final String COMPACTED_COUNT = "count_of_compacted_files";

    // Value to detect tombstones
    private static final int TOMBSTONE = -1;
    private static final int VERY_FIRST_FILE = 0;

    // Fresh files first
    // Created once at load read-only mapped files
    private List<MemorySegment> ssTables;
    // NewSharedScope support deterministic deallocation
    private final ResourceScope resourceScope;

    public static Storage load(Path basePath) throws IOException {
        ResourceScope scope = ResourceScope.newSharedScope();

        // Created compact file, but process stopped
        Path temp = basePath.resolve(COMPACT);
        if (Files.exists(temp)) {
            deleteAndMove(basePath, temp, scope, null);
        } else {
            // Clear directory if it exists
            if (Files.exists(basePath.resolve(COMPACT_DIR))) {
                clearCompactDirectory(basePath.resolve(COMPACT_DIR));
            }
        }

        List<MemorySegment> ssTables = getSsTables(basePath, scope);
        return new Storage(ssTables, scope);
    }

    private Storage(List<MemorySegment> ssTables, ResourceScope resourceScope) {
        this.ssTables = ssTables;
        this.resourceScope = resourceScope;
    }

    public Path save(Collection<BaseEntry<MemorySegment>> entries, Path basePath) throws IOException {
        if (entries.isEmpty()) {
            return null;
        }

        try (ResourceScope scope = ResourceScope.newConfinedScope()) {
            // Count of records + offsets
            long offset = Long.BYTES * (entries.size() + 1L);
            // Size of records + all meta
            long fileSize = offset + entries.size() * Long.BYTES * 2L;
            for (BaseEntry<MemorySegment> entry : entries) {
                fileSize += entry.key().byteSize();
                // value == null -> tombstone
                if (entry.value() != null) {
                    fileSize += entry.value().byteSize();
                }
            }

            long index = 0L;
            FileChannel.MapMode mode = FileChannel.MapMode.READ_WRITE;

            Path dataFile = basePath.resolve(DATA_NAME + getTimestamp() + DATA_EXT);
            createFile(dataFile);

            MemorySegment table = getMappedFile(dataFile, scope, fileSize, mode);

            // Set count of records at 0 index
            MemoryAccess.setLongAtIndex(table, index++, entries.size());
            // Set records
            for (BaseEntry<MemorySegment> entry : entries) {
                MemoryAccess.setLongAtIndex(table, index++, offset);
                offset = setEntry(table, entry, offset);
            }
            return dataFile;
        }
    }

    public void update(Path basePath) throws IOException {
        ssTables = getSsTables(basePath, resourceScope);
    }

    public BaseEntry<MemorySegment> get(MemorySegment key) {
        if (key == null) {
            return null;
        }

        for (MemorySegment table : ssTables) {
            long index = binarySearch(key, table);
            if (index >= 0) {
                BaseEntry<MemorySegment> result = entryAt(table, index);
                return result.value() == null ? null : result;
            }
        }
        return null;
    }

    public List<PeekIterator> allIterators(MemorySegment from, MemorySegment to) {
        List<PeekIterator> iterators = new ArrayList<>();
        long index = 1;

        // ssTables are guaranteed not to change while this method is called from compact()
        // Compact() is guaranteed to be executed only in one thread
        compactedFilesCount.set(ssTables.size());
        for (MemorySegment table : ssTables) {
            iterators.add(new PeekIterator(getIterator(table, from, to), index++));
        }
        return iterators;
    }

    public boolean compact(Iterator<BaseEntry<MemorySegment>> all, Path basePath) throws IOException {
        // Nothing to compact
        int tablesCount = compactedFilesCount.get();
        if (tablesCount == 0 || tablesCount == 1) {
            return false;
        }

        Collection<BaseEntry<MemorySegment>> entries = new ArrayList<>();
        all.forEachRemaining(entries::add);

        // Creating a directory for a compact file
        Path dir = basePath.resolve(COMPACT_DIR);
        Files.deleteIfExists(dir);
        Files.createDirectory(dir);

        // Set compacted file to safe delete and parallel execute with flush()
        Path count = dir.resolve(COMPACTED_COUNT);
        createFile(count);
        MemorySegment countFile = getMappedFile(count, resourceScope,
                Long.BYTES, FileChannel.MapMode.READ_WRITE);
        MemoryAccess.setLongAtIndex(countFile,0, compactedFilesCount.get());

        // Writing data
        Path compactFile = save(entries, dir);

        // Can be restored -> After Files.move reading new file
        Path temp = basePath.resolve(COMPACT);
        Files.move(Objects.requireNonNull(compactFile), temp, StandardCopyOption.ATOMIC_MOVE);

        // Delete old files + move temp file as default
        deleteAndMove(basePath, temp, resourceScope, countFile);
        return true;
    }

    // When a resource scope is closed. It is no longer alive
    // and subsequent operations on resources associated with that scope will fail
    @Override
    public void close() throws IOException {
        resourceScope.close();
    }

    private static void deleteAndMove(Path basePath, Path temp,
                                      ResourceScope scope, MemorySegment countFile) throws IOException {
        MemorySegment filesToDelete;
        Path countPath = basePath.resolve(COMPACT_DIR).resolve(COMPACTED_COUNT);
        if (countFile == null) {
            filesToDelete = getMappedFile(countPath, scope,
                    Long.BYTES, FileChannel.MapMode.READ_WRITE);
        } else {
            filesToDelete = countFile;
        }

        long count = MemoryAccess.getLongAtIndex(filesToDelete, 0);

        // Deleting old files and service data
        try (Stream<Path> str = Files.list(basePath)) {
            Path[] paths = str.filter(i -> i.toString().contains(DATA_EXT))
                    .sorted(java.util.Comparator.comparing(Path::toString))
                    .toArray(Path[]::new);

            for (int i = 0; i < count; i++) {
                Files.delete(paths[i]);
            }
        }

        // Exists only compact file after Files.move
        Path finalFile = basePath.resolve(DATA_NAME + VERY_FIRST_FILE + DATA_EXT);
        Files.move(temp, finalFile, StandardCopyOption.ATOMIC_MOVE);
        clearCompactDirectory(basePath.resolve(COMPACT_DIR));
    }

    private static void clearCompactDirectory(Path compact) throws IOException {
        Files.deleteIfExists(compact.resolve(COMPACT));
        Files.deleteIfExists(compact.resolve(COMPACTED_COUNT));
        Files.delete(compact);
    }

    private static MemorySegment getMappedFile(Path path,
                                               ResourceScope scope,
                                               long fileSize,
                                               FileChannel.MapMode mode) throws IOException {
        return MemorySegment.mapFile(
                path,
                0,
                fileSize,
                mode,
                scope
        );
    }

    private static List<MemorySegment> getSsTables(Path basePath, ResourceScope scope) throws IOException {
        List<MemorySegment> tables = new ArrayList<>();
        FileChannel.MapMode mode = FileChannel.MapMode.READ_ONLY;
        try (Stream<Path> str = Files.list(basePath)) {
            Path[] paths = str.filter(i -> i.toString().contains(DATA_EXT))
                    .sorted(Comparator::pathsCompare)
                    .toArray(Path[]::new);

            for (Path path : paths) {
                tables.add(getMappedFile(path, scope, Files.size(path), mode));
            }
        }
        Collections.reverse(tables);
        return tables;
    }

    private long indexForRange(MemorySegment ssTable, MemorySegment key) {
        long index = binarySearch(key, ssTable);
        if (index < 0) {
            return ~index;
        }
        return index;
    }

    private long getTimestamp() {
        return System.nanoTime();
    }

    private Iterator<BaseEntry<MemorySegment>> getIterator(MemorySegment ssTable,
                                                           MemorySegment from, MemorySegment to) {
        long boarder = MemoryAccess.getLongAtIndex(ssTable, 0);

        long fromIndex = from == null ? 1 : indexForRange(ssTable, from);
        long toIndex = to == null ? boarder + 1 : indexForRange(ssTable, to);

        return new Iterator<>() {
            long current = fromIndex;

            @Override
            public boolean hasNext() {
                return current < toIndex;
            }

            @Override
            public BaseEntry<MemorySegment> next() {
                return entryAt(ssTable, current++);
            }
        };
    }

    private BaseEntry<MemorySegment> entryAt(MemorySegment ssTable, long index) {
        long offset = MemoryAccess.getLongAtIndex(ssTable, index);

        long keySize = MemoryAccess.getLongAtOffset(ssTable, offset);
        offset += Long.BYTES;
        MemorySegment key = ssTable.asSlice(offset, keySize);
        offset += keySize;

        long valueSize = MemoryAccess.getLongAtOffset(ssTable, offset);
        offset += Long.BYTES;
        MemorySegment value = valueSize == TOMBSTONE ? null : ssTable.asSlice(offset, valueSize);

        return new BaseEntry<>(key, value);
    }

    private long setEntry(MemorySegment ssTable, BaseEntry<MemorySegment> entry, long start) {
        long offset = start;
        MemoryAccess.setLongAtOffset(ssTable, offset, entry.key().byteSize());
        offset += Long.BYTES;

        ssTable.asSlice(offset).copyFrom(entry.key());
        offset += entry.key().byteSize();

        long valueSize = entry.value() == null ? TOMBSTONE : entry.value().byteSize();
        MemoryAccess.setLongAtOffset(ssTable, offset, valueSize);
        offset += Long.BYTES;

        if (valueSize != TOMBSTONE) {
            ssTable.asSlice(offset).copyFrom(entry.value());
            offset += entry.value().byteSize();
        }
        return offset;
    }

    // Return index with offset
    private long binarySearch(MemorySegment key, MemorySegment ssTable) {
        long left = 1;
        long right = MemoryAccess.getLongAtIndex(ssTable, 0);

        while (left <= right) {
            long mid = (left + right) >>> 1;
            BaseEntry<MemorySegment> current = entryAt(ssTable, mid);
            int result = Comparator.compare(current.key(), key);
            if (result < 0) {
                left = mid + 1;
            } else if (result > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }
        return ~left;
    }

    private void createFile(Path path) throws IOException {
        Files.deleteIfExists(path);
        Files.createFile(path);
    }
}
