package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

class MemorySegmentWriter {
    private int arrayIndex;
    private long lastSize;
    private long lastIndex;
    private final MemorySegment mappedMemorySegmentForStorage;
    private final MemorySegment mappedMemorySegmentForIndexes;

    MemorySegmentWriter(int arraySize, long storageSize, Utils utils) throws IOException {
        mappedMemorySegmentForStorage = createMappedSegment(utils.getStoragePath(), storageSize);
        long longSize = 8;
        mappedMemorySegmentForIndexes = createMappedSegment(utils.getIndexesPath(), longSize * (arraySize * 2L + 1));
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
        lastIndex += size;
        lastSize = size;
        arrayIndex++;
        long longSize = 8;
        MemoryAccess.setLong(mappedMemorySegmentForIndexes.asSlice(arrayIndex * longSize, longSize), lastIndex);
    }

    private void writeData(MemorySegment other) {
        writeToMappedMemorySegment(mappedMemorySegmentForStorage, lastIndex - lastSize, lastSize, other);
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
