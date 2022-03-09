package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.stream.LongStream;

final class SortedStringTable {
    public static final String INDEX_FILENAME = "index";
    public static final String DATA_FILENAME = "data";
    private static final Comparator<MemorySegment> lexicographically = new LexicographicMemorySegmentComparator();

    private final Path indexFile; // unload
    private final MemorySegmentAllocator segmentLazy;
    // Either dataSegment and offsets both null or both non-null
    private MemorySegment dataSegment;
    private long[] offsets;

    private SortedStringTable(Path indexFile, MemorySegmentAllocator segmentLazy) {
        this.indexFile = indexFile;
        this.segmentLazy = segmentLazy;
    }

    /**
     * Constructs SortedStringTable.
     */
    public static SortedStringTable of(
            Path folderPath,
            ResourceScope dataScope
    ) {
        Path indexFile = folderPath.resolve(INDEX_FILENAME);
        Path dataFile = folderPath.resolve(DATA_FILENAME);

        return new SortedStringTable(indexFile, (long size) -> MemorySegment.mapFile(
                createFileIfNotExists(dataFile),
                0L,
                size,
                FileChannel.MapMode.READ_WRITE,
                dataScope
        ));
    }

    // write key-value pairs in format:
    // ┌─────────────────────┬─────────────────────────┐
    // │key: byte[keySize(i)]│value: byte[valueSize(i)]│ entriesMapped() times
    // └─────────────────────┴─────────────────────────┘
    public SortedStringTable write(Collection<BaseEntry<MemorySegment>> entries) throws IOException {
        offsets = calculateOffsets(entries);
        dataSegment = segmentLazy.allocate(offsets[offsets.length - 1]);
        int i = 0;
        for (BaseEntry<MemorySegment> entry : entries) {
            mappedKey(i).copyFrom(entry.key());
            mappedValue(i).copyFrom(entry.value());
            i++;
        }
        return this;
    }

    // binary search
    public BaseEntry<MemorySegment> get(MemorySegment key) throws IOException {
        if (offsets == null && dataSegment == null) {
            loadFromFiles();
        }
        int low = 0;
        int high = entriesMapped() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            MemorySegment midVal = mappedKey(mid);
            if (lexicographically.compare(midVal, key) < 0) {
                low = mid + 1;
            } else if (lexicographically.compare(midVal, key) > 0) {
                high = mid - 1;
            } else {
                return new BaseEntry<>(midVal, mappedValue(mid)); // key found
            }
        }
        return null; // key not found.
    }

    public void unload() throws IOException {
        try (LongArrayOutput out = LongArrayOutput.of(indexFile)) {
            out.write(offsets);
        }
        offsets = null;
        dataSegment.unload();
        dataSegment = null;
    }

    private static long[] calculateOffsets(Collection<BaseEntry<MemorySegment>> entries) {
        long[] result = LongStream.concat(
                LongStream.of(0L),
                entries
                        .stream()
                        // alternating key and value sizes
                        .flatMapToLong(entry -> LongStream.of(entry.key().byteSize(), entry.value().byteSize()))
        ).toArray();
        Arrays.parallelPrefix(result, Long::sum);
        return result;
    }

    private void loadFromFiles() throws IOException {
        try (LongArrayInput in = LongArrayInput.of(indexFile)) {
            offsets = in.readLongArray();
        }
        dataSegment = segmentLazy.allocate(offsets[offsets.length - 1]);
    }

    private long keyOffset(int i) {
        return offsets[i << 1];
    }

    private long valueOffset(int i) {
        return offsets[(i << 1) | 1];
    }

    private long keySize(int i) {
        return valueOffset(i) - keyOffset(i);
    }

    private long valueSize(int i) {
        return keyOffset(i + 1) - valueOffset(i);
    }

    private int entriesMapped() {
        return offsets.length >> 1;
    }

    private MemorySegment mappedKey(int i) {
        return dataSegment.asSlice(keyOffset(i), keySize(i));
    }

    private MemorySegment mappedValue(int i) {
        return dataSegment.asSlice(valueOffset(i), valueSize(i));
    }

    private static Path createFileIfNotExists(Path path) throws IOException {
        try {
            return Files.createFile(path);
        } catch (FileAlreadyExistsException ignored) {
            return path;
        }
    }
}
