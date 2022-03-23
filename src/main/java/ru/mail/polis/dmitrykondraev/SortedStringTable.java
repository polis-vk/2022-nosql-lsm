package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import static ru.mail.polis.dmitrykondraev.MemorySegmentComparator.LEXICOGRAPHICALLY;

final class SortedStringTable implements Closeable {
    public static final String INDEX_FILENAME = "index";
    public static final String DATA_FILENAME = "data";


    private final Path indexFile;
    private final Path dataFile;
    // Either dataSegment and offsets both null or both non-null
    private MemorySegment dataSegment;
    private MemorySegment indexSegment;
    private final ResourceScope scope;

    private SortedStringTable(Path indexFile, Path dataFile, ResourceScope scope) {
        this.indexFile = indexFile;
        this.dataFile = dataFile;
        this.scope = scope;
    }

    /**
     * Constructs SortedStringTable.
     */
    public static SortedStringTable of(Path folderPath) {
        return new SortedStringTable(
                folderPath.resolve(INDEX_FILENAME),
                folderPath.resolve(DATA_FILENAME),
                ResourceScope.newSharedScope()
        );
    }

    public SortedStringTable write(Collection<MemorySegmentEntry> entries) throws IOException {
        writeIndex(entries);
        dataSegment = MemorySegment.mapFile(
                createFileIfNotExists(dataFile),
                0L,
                dataSize(),
                FileChannel.MapMode.READ_WRITE,
                scope
        );
        int i = 0;
        for (MemorySegmentEntry entry : entries) {
            entry.copyTo(mappedEntrySegment(i));
            i++;
        }
        return this;
    }

    /**
     * left binary search
     *
     * @param first inclusive
     * @param last  exclusive
     * @return first index such that key of entry with that index is equal to key,
     * if no such index exists, result < 0
     */
    private int binarySearch(int first, int last, MemorySegment key) {
        while (first < last) {
            int mid = first + (last - first) / 2;
            int compare = LEXICOGRAPHICALLY.compare(mappedEntry(mid).key(), key);
            if (compare < 0) {
                first = mid + 1;
            } else if (compare > 0) {
                last = mid;
            } else {
                return mid;
            }
        }
        return -(first + 1);
    }

    /**
     * @return null if either indexFile or dataFile does not exist,
     * null if key does not exist in table
     * @throws IOException if other I/O error occurs
     */
    public MemorySegmentEntry get(MemorySegment key) throws IOException {
        if (indexSegment == null && dataSegment == null) {
            loadFromFiles(); // throws NoSuchFileException
        }
        int index = binarySearch(0, entriesMapped(), key);
        return index < 0 ? null : mappedEntry(index);
    }

    public Iterator<MemorySegmentEntry> get(MemorySegment from, MemorySegment to) throws IOException {
        if (indexSegment == null && dataSegment == null) {
            loadFromFiles(); // throws NoSuchFileException
        }
        if (from == null) {
            from = mappedEntry(0).key();
        }
        if (to != null && LEXICOGRAPHICALLY.compare(from, to) >= 0) {
            return Collections.emptyIterator();
        }
        // TODO fix to
        return new IteratorImpl(from, to);
    }

    @Override
    public void close() {
        scope.close();
        indexSegment = null;
        dataSegment = null;
    }

    private void loadFromFiles() throws IOException {
        if (indexSegment != null || dataSegment != null) {
            throw new IllegalStateException("Can't load if already mapping");
        }
        indexSegment = MemorySegment.mapFile(
                indexFile,
                0L,
                Files.size(indexFile),
                FileChannel.MapMode.READ_ONLY,
                scope
        );
        dataSegment = MemorySegment.mapFile(
                dataFile,
                0L,
                dataSize(),
                FileChannel.MapMode.READ_ONLY,
                scope
        );
    }

    private long entryOffset(long i) {
        return MemoryAccess.getLongAtOffset(indexSegment, Integer.BYTES + i * Long.BYTES);
    }

    private long entrySize(long i) {
        return entryOffset(i + 1) - entryOffset(i);
    }

    private int entriesMapped() {
        return MemoryAccess.getIntAtOffset(indexSegment, 0L);
    }

    private long dataSize() {
        return entryOffset(entriesMapped());
    }

    private MemorySegment mappedEntrySegment(long i) {
        return dataSegment.asSlice(entryOffset(i), entrySize(i));
    }

    private MemorySegmentEntry mappedEntry(long i) {
        return MemorySegmentEntry.of(mappedEntrySegment(i));
    }

    /**
     * write offsets in format:
     * ┌─────────┬─────────────────┐
     * │size: int│array: long[size]│
     * └─────────┴─────────────────┘
     * where size is number of entries and
     * array represents offsets of entries in data file specified by methods
     * keyOffset, valueOffset, keySize and valueSize.
     */
    private void writeIndex(Collection<MemorySegmentEntry> entries) throws IOException {
        indexSegment = MemorySegment.mapFile(
                createFileIfNotExists(indexFile),
                0L,
                Integer.BYTES + (1L + entries.size()) * Long.BYTES,
                FileChannel.MapMode.READ_WRITE,
                scope
        );
        MemoryAccess.setInt(indexSegment, entries.size());
        MemorySegment offsetsSegment = indexSegment.asSlice(Integer.BYTES);
        long currentOffset = 0L;
        long index = 0L;
        MemoryAccess.setLongAtIndex(offsetsSegment, index++, currentOffset);
        for (MemorySegmentEntry entry : entries) {
            currentOffset += entry.bytesSize();
            MemoryAccess.setLongAtIndex(offsetsSegment, index++, currentOffset);
        }
        indexSegment = indexSegment.asReadOnly();
    }

    private static Path createFileIfNotExists(Path path) throws IOException {
        try {
            return Files.createFile(path);
        } catch (FileAlreadyExistsException ignored) {
            return path;
        }
    }

    private final class IteratorImpl implements java.util.Iterator<MemorySegmentEntry> {
        private final MemorySegment from;
        private final MemorySegment to;

        private int first = 0;
        private int last = entriesMapped();
        private boolean pivoted = false;

        private IteratorImpl(MemorySegment from, MemorySegment to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean hasNext() {
            if (pivoted) {
                return first < last;
            }
            first = binarySearch(first, last, from);
            if (first < 0) {
                first = -(first + 1);
            }
            if (first >= last) {
                pivoted = true;
                return false;
            }
            if (to != null) {
                last = binarySearch(first, last, to);
                if (last < 0) {
                    last = -(last + 1);
                }
            }
            pivoted = true;
            return first < last;
        }

        @Override
        public MemorySegmentEntry next() {
            return mappedEntry(first++);
        }
    }
}
