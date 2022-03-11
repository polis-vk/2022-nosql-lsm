package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class Converter {

    private Path pathToOffsets;
    private Path pathToEntries;
    private MemorySegment mappedSegmentEntries;
    private MemorySegment mappedSegmentOffsets;
    private long[] offsets;
    private int idx;

    public void startSerializeEntries(long dataCount, long fileSize, Path path) throws IOException {
        offsets = new long[(int) (dataCount * 2 + 1)];
        offsets[0] = 0;

        if (dataCount == 0) {
            return;
        }

        createPaths(path);
        if (Files.notExists(pathToEntries)) {
            Files.createFile(pathToEntries);
            Files.createFile(pathToOffsets);
        }

        mappedSegmentEntries = newMapped(pathToEntries, fileSize);
        mappedSegmentOffsets = newMapped(pathToOffsets, Long.BYTES * (dataCount * 2 + 1));
    }

    public BaseEntry<MemorySegment> searchEntry(Path path, MemorySegment key) throws IOException {
        createPaths(path);

        if (Files.notExists(pathToEntries)) {
            return null;
        }

        long offsetsSize = Files.size(pathToOffsets);
        mappedSegmentEntries = newMapped(pathToEntries, Files.size(pathToEntries));
        mappedSegmentOffsets = newMapped(pathToOffsets, offsetsSize);

        SegmentsComparator comparator = new SegmentsComparator();
        MemorySegment currentKey;
        long size;
        long currentLong;
        long nextLong;
        for (long i = 0; i < (offsetsSize - Long.BYTES) / 8; i++) {
            currentLong = MemoryAccess.getLongAtIndex(mappedSegmentOffsets, i);
            nextLong = MemoryAccess.getLongAtIndex(mappedSegmentOffsets, i + 1);
            size = nextLong - currentLong;
            currentKey = mappedSegmentEntries.asSlice(currentLong, size);
            if (comparator.compare(key, currentKey) == 0) {
                size = MemoryAccess.getLongAtIndex(mappedSegmentOffsets, i + 2) - nextLong;
                return new BaseEntry<>(currentKey, mappedSegmentEntries.asSlice(nextLong, size));
            }
        }
        return null;
    }

    public void writeOffsets(long size) {
        if (size == 0) {
            return;
        }
        mappedSegmentOffsets.asSlice(0L, (long) offsets.length * Long.BYTES)
                .copyFrom(MemorySegment.ofArray(offsets));
    }

    public void writeEntries(BaseEntry<MemorySegment> data, long keySize, long valueSize) {
        offsets[idx + 1] = offsets[idx] + keySize;
        offsets[idx + 2] = offsets[idx + 1] + valueSize;
        mappedSegmentEntries.asSlice(offsets[idx], keySize).copyFrom(data.key());
        mappedSegmentEntries.asSlice(offsets[idx + 1], valueSize).copyFrom(data.value());

        idx += 2;
    }

    private void createPaths(Path path) {
        pathToOffsets = path.resolve(String.valueOf(FileName.OFFSETS.getName()));
        pathToEntries = path.resolve(String.valueOf(FileName.SAVED_DATA.getName()));
    }

    private MemorySegment newMapped(Path path, long size) throws IOException {
        return MemorySegment.mapFile(
                path,
                0,
                size,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.newConfinedScope()
        );
    }
}
