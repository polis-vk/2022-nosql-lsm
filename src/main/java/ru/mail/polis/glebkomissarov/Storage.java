package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

class Storage implements Closeable {

    // File structure
    // [Entries count][Entry1 offset]...[EntryN offset] | [KeySize][Key][ValueSize][Value]
    private static final String DATA_NAME = "data";
    private static final String DATA_EXT = ".dat";

    private static final String COMPACT_DIR = "compact";
    private static final String COMPACT = "ready_to_read";

    // Value to detect tombstones
    private static final int TOMBSTONE = -1;

    // Fresh files first
    // Created once at load read-only mapped files
    private final List<MemorySegment> SSTables;
    // NewSharedScope support deterministic deallocation
    private final ResourceScope scope;

    public static Storage load(Path basePath) throws IOException {
        List<MemorySegment> SSTables = new ArrayList<>();
        ResourceScope scope = ResourceScope.newSharedScope();
        FileChannel.MapMode mode = FileChannel.MapMode.READ_ONLY;

        // Created compact file, but process stopped
        Path temp = basePath.resolve(COMPACT);
        if (Files.exists(temp)) {
            deleteAndMove(basePath, temp);
        }

        for (int i = 0; ; i++) {
            Path dataFile = basePath.resolve(DATA_NAME + i + DATA_EXT);
            // Minimize count of system calls
            try {
                long dataSize = Files.size(dataFile);
                SSTables.add(getMappedFile(dataFile, scope, dataSize, mode));
            } catch (NoSuchFileException e) {
                break;
            }
        }

        Collections.reverse(SSTables);
        return new Storage(SSTables, scope);
    }

    private Storage(List<MemorySegment> SSTables, ResourceScope scope) {
        this.SSTables = SSTables;
        this.scope = scope;
    }

    public Path save(Collection<BaseEntry<MemorySegment>> entries, Path basePath) throws IOException {
        if (entries.isEmpty()) {
            return null;
        }

        try(ResourceScope scope = ResourceScope.newConfinedScope()) {
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

            Path dataFile = basePath.resolve(DATA_NAME + SSTablesCount() + DATA_EXT);
            Files.deleteIfExists(dataFile);
            Files.createFile(dataFile);

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

    public BaseEntry<MemorySegment> get(MemorySegment key) {
        if (key == null) {
            return null;
        }

        for (MemorySegment table : SSTables) {
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

        for (MemorySegment table : SSTables) {
            iterators.add(new PeekIterator(getIterator(table, from, to), index++));
        }
        return iterators;
    }

    public long SSTablesCount() {
        return SSTables.size();
    }

    public boolean compact(Iterator<BaseEntry<MemorySegment>> all,
                           Path basePath,
                           boolean isEmpty) throws IOException {
        // Nothing to compact
        if (SSTablesCount() == 0 || (SSTablesCount() == 1 && isEmpty)) {
            return false;
        }

        Collection<BaseEntry<MemorySegment>> entries = new ArrayList<>();
        all.forEachRemaining(entries::add);

        // Creating a directory for a compact file
        Path dir = basePath.resolve(COMPACT_DIR);
        Files.deleteIfExists(dir);
        Files.createDirectory(dir);

        // Writing data
        Path compactFile = save(entries, dir);

        // Can be restored -> After Files.move reading new file
        Path temp = basePath.resolve(COMPACT);
        Files.move(compactFile, temp, StandardCopyOption.ATOMIC_MOVE);

        // Delete old files + move temp file as default
        deleteAndMove(basePath, temp);

        return true;
    }

    /**
     * When a resource scope is closed, it is no longer alive
     * and subsequent operations on resources associated with that scope will fail
     */
    @Override
    public void close() throws IOException {
        scope.close();
    }

    public boolean isClosed() {
        return !scope.isAlive();
    }

    private static void deleteAndMove(Path basePath, Path temp) throws IOException {
        // Deleting old files
        for (int i = 0; ; i++) {
            try {
                Files.delete(basePath.resolve(DATA_NAME + i + DATA_EXT));
            } catch (NoSuchFileException e) {
                break;
            }
        }

        // Exists only compact file after Files.move
        Path finalFile = basePath.resolve(DATA_NAME + 0 + DATA_EXT);
        Files.move(temp, finalFile, StandardCopyOption.ATOMIC_MOVE);
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

    private long indexForRange(MemorySegment SSTable, MemorySegment key) {
        long index = binarySearch(key, SSTable);
        if (index < 0) {
            return ~index;
        }
        return index;
    }

    private Iterator<BaseEntry<MemorySegment>> getIterator(MemorySegment SSTable,
                                                           MemorySegment from, MemorySegment to) {
        long boarder = MemoryAccess.getLongAtIndex(SSTable, 0);

        long fromIndex = from == null ? 1 : indexForRange(SSTable, from);
        long toIndex = to == null ? boarder + 1 : indexForRange(SSTable, to);

        return new Iterator<>() {
            long current = fromIndex;

            @Override
            public boolean hasNext() {
                return current < toIndex;
            }

            @Override
            public BaseEntry<MemorySegment> next() {
                return entryAt(SSTable, current++);
            }
        };
    }

    private BaseEntry<MemorySegment> entryAt(MemorySegment SSTable, long index) {
        long offset = MemoryAccess.getLongAtIndex(SSTable, index);

        long keySize = MemoryAccess.getLongAtOffset(SSTable, offset);
        offset += Long.BYTES;
        MemorySegment key = SSTable.asSlice(offset, keySize);
        offset += keySize;

        long valueSize = MemoryAccess.getLongAtOffset(SSTable, offset);
        offset += Long.BYTES;
        MemorySegment value = valueSize == TOMBSTONE ? null : SSTable.asSlice(offset, valueSize);

        return new BaseEntry<>(key, value);
    }

    private long setEntry(MemorySegment SSTable, BaseEntry<MemorySegment> entry, long offset) {
        MemoryAccess.setLongAtOffset(SSTable, offset, entry.key().byteSize());
        offset += Long.BYTES;

        SSTable.asSlice(offset).copyFrom(entry.key());
        offset += entry.key().byteSize();

        long valueSize = entry.value() == null ? TOMBSTONE : entry.value().byteSize();
        MemoryAccess.setLongAtOffset(SSTable, offset, valueSize);
        offset += Long.BYTES;

        if (valueSize != TOMBSTONE) {
            SSTable.asSlice(offset).copyFrom(entry.value());
            offset += entry.value().byteSize();
        }
        return offset;
    }

    // Return index with offset
    private long binarySearch(MemorySegment key, MemorySegment SSTable) {
        long left = 1;
        long right = MemoryAccess.getLongAtIndex(SSTable, 0);

        while (left <= right) {
            long mid = (left + right) >>> 1;
           BaseEntry<MemorySegment> current = entryAt(SSTable, mid);
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
}
