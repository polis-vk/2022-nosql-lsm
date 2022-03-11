package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
<<<<<<< HEAD
=======
import org.jetbrains.annotations.Nullable;
>>>>>>> origin/stage_2
import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

<<<<<<< HEAD
public class Converter {

    private Path pathToOffsets;
    private Path pathToEntries;
    private MemorySegment mappedSegmentEntries;
    private MemorySegment mappedSegmentOffsets;
    private long[] offsets;
    private int idx;

    public void startSerializeEntries(long dataCount, long fileSize, Path path) throws IOException {
=======
public final class Converter {

    private static Path pathToOffsets;
    private static Path pathToEntries;
    private static MemorySegment mappedSegmentEntries;
    private static MemorySegment mappedSegmentOffsets;
    private static long[] offsets;
    private static int idx;

    private Converter() {

    }

    public static void startSerializeEntries(long dataCount, long fileSize, Path path) throws IOException {
>>>>>>> origin/stage_2
        if (dataCount == 0) {
            return;
        }

        createPaths(path);
        if (Files.notExists(pathToEntries)) {
            Files.createFile(pathToEntries);
            Files.createFile(pathToOffsets);
        }

        offsets = new long[(int) (dataCount * 2 + 1)];
        offsets[0] = 0;

        mappedSegmentEntries = newMapped(pathToEntries, fileSize);
        mappedSegmentOffsets = newMapped(pathToOffsets, Long.SIZE * dataCount);
    }

<<<<<<< HEAD
    public BaseEntry<MemorySegment> searchEntry(Path path, MemorySegment key) throws IOException {
=======
    @Nullable
    public static BaseEntry<MemorySegment> searchEntry(Path path, MemorySegment key) throws IOException {
>>>>>>> origin/stage_2
        createPaths(path);

        if (Files.notExists(pathToEntries)) {
            return null;
        }

        mappedSegmentEntries = newMapped(pathToEntries, Files.size(pathToEntries));
        mappedSegmentOffsets = newMapped(pathToOffsets, Files.size(pathToOffsets));

        long[] offsets = mappedSegmentOffsets.toLongArray();

        SegmentsComparator comparator = new SegmentsComparator();
        MemorySegment currentKey;
        long size;
        for (int i = 0; i < offsets.length - 1; i += 2) {
            size = offsets[i + 1] - offsets[i];
            currentKey = mappedSegmentEntries.asSlice(offsets[i], size);
            if (comparator.compare(key, currentKey) == 0) {
                size = offsets[i + 2] - offsets[i + 1];
                return new BaseEntry<>(currentKey, mappedSegmentEntries.asSlice(offsets[i + 1], size));
            }
        }
        return null;
    }

<<<<<<< HEAD
    public void writeOffsets() {
        mappedSegmentOffsets.asSlice(0L, (long) offsets.length * Long.SIZE / 8)
                .copyFrom(MemorySegment.ofArray(offsets));
    }

    public void writeEntries(BaseEntry<MemorySegment> data, long keySize, long valueSize) {
=======
    public static void writeOffsets() {
        mappedSegmentOffsets.asSlice(0L, (long) offsets.length * Long.SIZE / 8)
                .copyFrom(MemorySegment.ofArray(offsets));
        idx = 0;
    }

    public static void writeEntries(BaseEntry<MemorySegment> data, long keySize, long valueSize) {
>>>>>>> origin/stage_2
        offsets[idx + 1] = offsets[idx] + keySize;
        offsets[idx + 2] = offsets[idx + 1] + valueSize;

        mappedSegmentEntries.asSlice(offsets[idx], keySize).copyFrom(data.key());
        mappedSegmentEntries.asSlice(offsets[idx + 1], valueSize).copyFrom(data.value());

        idx += 2;
    }

<<<<<<< HEAD
    private void createPaths(Path path) {
=======
    private static void createPaths(Path path) {
>>>>>>> origin/stage_2
        pathToOffsets = path.resolve(String.valueOf(FileNames.OFFSETS.getName()));
        pathToEntries = path.resolve(String.valueOf(FileNames.SAVED_DATA.getName()));
    }

<<<<<<< HEAD
    private MemorySegment newMapped(Path path, long size) throws IOException {
=======
    private static MemorySegment newMapped(Path path, long size) throws IOException {
>>>>>>> origin/stage_2
        return MemorySegment.mapFile(
                path,
                0,
                size,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.newConfinedScope()
        );
    }
}
