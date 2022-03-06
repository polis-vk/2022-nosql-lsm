package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.channels.FileChannel;

class MemorySegmentWriter {
    private static final long longSize = 8;
    private final long[] indexes;
    private int arrayIndex = 0;
    private final MemorySegment mappedMemorySegment;
    Utils utils;

    MemorySegmentWriter(int arraySize, long storageSize, Utils utils) throws IOException {
        this.utils = utils;
        indexes = new long[arraySize * 2 + 1];
        mappedMemorySegment = MemorySegment.mapFile(utils.getStoragePath(), 0, storageSize, FileChannel.MapMode.READ_WRITE, ResourceScope.globalScope());
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
    }

    private void writeData(MemorySegment other) {
        long byteOffset = indexes[arrayIndex - 1];
        long size = indexes[arrayIndex] - byteOffset;
        writeMemorySegment(byteOffset, size, other);
    }

    void saveIndexes() throws IOException {
        MemorySegment mapped = MemorySegment.mapFile(
                utils.getIndexesPath(),
                0,
                longSize * indexes.length,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.globalScope()
        );
        mapped.copyFrom(MemorySegment.ofArray(indexes));
        mapped.load();
    }

    private void writeMemorySegment(long byteOffset, long byteSize, MemorySegment other) {
        mappedMemorySegment.asSlice(byteOffset, byteSize).copyFrom(other);
    }

    void saveMemorySegments() {
        mappedMemorySegment.load();
    }
}
