package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;

class MemorySegmentWriter {
    private final long[] indexes;
    private int arrayIndex;
    private final MemorySegment mappedMemorySegmentForStorage;
    private final MemorySegment mappedMemorySegmentForIndexes;

    MemorySegmentWriter(int arraySize, long storageSize, Utils utils) throws IOException {
        indexes = new long[arraySize * 2 + 1];
        mappedMemorySegmentForStorage = createMappedSegment(utils.getStoragePath(), storageSize);
        long longSize = 8;
        mappedMemorySegmentForIndexes = createMappedSegment(utils.getIndexesPath(), longSize * indexes.length);
    }

    private MemorySegment createMappedSegment(Path path, long size) throws IOException {
        return MemorySegment.mapFile(
                path,
                0,
                size,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.globalScope()
        );
    }

    void writeEntry(BaseEntry<MemorySegment> entry) {
        MemorySegment key = entry.key();
        writeIndex(key.byteSize());
        writeData(key);
        MemorySegment value = entry.value();
        writeIndex(value.byteSize());
        writeData(value);
    }

    private void writeIndex(long size) {
        indexes[arrayIndex + 1] = indexes[arrayIndex] + size;
        arrayIndex++;
        long longSize = 8;
        writeToMappedMemorySegment(mappedMemorySegmentForIndexes,
                arrayIndex * longSize,
                longSize,
                MemorySegment.ofArray(Arrays.copyOfRange(indexes, arrayIndex, arrayIndex + 1))
        );
    }

    private void writeData(MemorySegment other) {
        long byteOffset = indexes[arrayIndex - 1];
        long size = indexes[arrayIndex] - byteOffset;
        writeToMappedMemorySegment(mappedMemorySegmentForStorage, byteOffset, size, other);
    }

    private void writeToMappedMemorySegment(MemorySegment mapped, long byteOffset, long byteSize, MemorySegment other) {
        mapped.asSlice(byteOffset, byteSize).copyFrom(other);
    }


    void saveMemorySegments() {
        mappedMemorySegmentForStorage.load();
    }

    void saveIndexes() {
        mappedMemorySegmentForIndexes.load();
    }
}
