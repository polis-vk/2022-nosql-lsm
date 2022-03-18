package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;

final class SortedStringTable implements Closeable {
    public static final String INDEX_FILENAME = "index";
    public static final String DATA_FILENAME = "data";
    private static final Comparator<MemorySegment> lexicographically = new MemorySegmentComparator();

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

    /**
     * Write key-value pairs in format:
     * ┌─────────────────────┬─────────────────────────┐
     * │key: byte[keySize(i)]│value: byte[valueSize(i)]│ entriesMapped() times.
     * └─────────────────────┴─────────────────────────┘
     */
    public SortedStringTable write(Collection<Entry<MemorySegment>> entries) throws IOException {
        writeIndex(entries);
        dataSegment = MemorySegment.mapFile(
                createFileIfNotExists(dataFile),
                0L,
                dataSize(),
                FileChannel.MapMode.READ_WRITE,
                scope
        );
        int i = 0;
        for (Entry<MemorySegment> entry : entries) {
            mappedKey(i).copyFrom(entry.key());
            mappedValue(i).copyFrom(entry.value());
            i++;
        }
        return this;
    }

    /**
     * Performs binary search.
     * @return null if either indexFile or dataFile does not exist,
     *         null if key does not exist in table
     * @throws IOException if other I/O error occurs
     */
    public BaseEntry<MemorySegment> get(MemorySegment key) throws IOException {
        if (indexSegment == null && dataSegment == null) {
            try {
                loadFromFiles();
            } catch (NoSuchFileException ignored) {
                return null;
            }
        }
        int low = 0;
        int high = entriesMapped() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            MemorySegment midVal = mappedKey(mid);
            int compare = lexicographically.compare(midVal, key);
            if (compare < 0) {
                low = mid + 1;
            } else if (compare > 0) {
                high = mid - 1;
            } else {
                return new BaseEntry<>(midVal, mappedValue(mid)); // key found
            }
        }
        return null; // key not found.
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

    private long keyOffset(long i) {
        return MemoryAccess.getLongAtOffset(indexSegment, Integer.BYTES + (i << 1) * Long.BYTES);
    }

    private long valueOffset(long i) {
        return MemoryAccess.getLongAtOffset(indexSegment, Integer.BYTES + ((i << 1) | 1L) * Long.BYTES);
    }

    private long keySize(long i) {
        return valueOffset(i) - keyOffset(i);
    }

    private long valueSize(long i) {
        return keyOffset(i + 1) - valueOffset(i);
    }

    private int entriesMapped() {
        return MemoryAccess.getIntAtOffset(indexSegment, 0L);
    }

    private long dataSize() {
        return keyOffset(entriesMapped());
    }

    private MemorySegment mappedKey(long i) {
        return dataSegment.asSlice(keyOffset(i), keySize(i));
    }

    private MemorySegment mappedValue(long i) {
        return dataSegment.asSlice(valueOffset(i), valueSize(i));
    }

    /**
     * write offsets in format:
     * ┌─────────┬─────────────────────────┐
     * │size: int│array: long[size * 2 + 1]│
     * └─────────┴─────────────────────────┘
     * where size is number of entries and
     * array represents offsets of keys and values in data file specified by methods
     * keyOffset, valueOffset, keySize and valueSize.
     */
    private void writeIndex(Collection<Entry<MemorySegment>> entries) throws IOException {
        indexSegment = MemorySegment.mapFile(
                createFileIfNotExists(indexFile),
                0L,
                Integer.BYTES + (entries.size() * 2L + 1) * Long.BYTES,
                FileChannel.MapMode.READ_WRITE,
                scope
        );
        MemoryAccess.setInt(indexSegment, entries.size());
        MemorySegment offsetsSegment = indexSegment.asSlice(Integer.BYTES);
        long currentOffset = 0L;
        long index = 0L;
        MemoryAccess.setLongAtIndex(offsetsSegment, index++, currentOffset);
        for (Entry<MemorySegment> entry : entries) {
            currentOffset += entry.key().byteSize();
            MemoryAccess.setLongAtIndex(offsetsSegment, index++, currentOffset);
            currentOffset += entry.value().byteSize();
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
}
