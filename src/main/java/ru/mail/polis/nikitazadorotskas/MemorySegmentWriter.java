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
    private final ResourceScope scope;

    MemorySegmentWriter(int arraySize, long storageSize, Utils utils, ResourceScope scope, int number)
            throws IOException {
        this.scope = scope;
        mappedMemorySegmentForStorage = createMappedSegment(utils.getStoragePath(number), storageSize);
        mappedMemorySegmentForIndexes = createMappedSegment(
                utils.getIndexesPath(number),
                Long.BYTES * (arraySize * 2L + 1)
        );
    }

    private MemorySegment createMappedSegment(Path path, long size) throws IOException {
        return MemorySegment.mapFile(path, 0, size, FileChannel.MapMode.READ_WRITE, scope);
    }

    void writeEntry(BaseEntry<MemorySegment> entry) {
        writePartOfEntry(entry.key());
        writePartOfEntry(entry.value());
    }

    void writePartOfEntry(MemorySegment data) {
        if (data == null) {
            markIndexOfNullValue();
            return;
        }
        writeIndex(data.byteSize());
        writeData(data);
    }

    private void writeIndex(long size) {
        lastIndex += size;
        lastSize = size;
        setIndex(lastIndex);
    }

    private void markIndexOfNullValue() {
        setIndex(-1);
    }

    private void setIndex(long index) {
        MemoryAccess.setLongAtIndex(mappedMemorySegmentForIndexes, ++arrayIndex, index);
    }

    private void writeData(MemorySegment other) {
        writeToMappedMemorySegment(mappedMemorySegmentForStorage, lastIndex - lastSize, lastSize, other);
    }

    private void writeToMappedMemorySegment(MemorySegment mapped, long byteOffset, long byteSize, MemorySegment other) {
        mapped.asSlice(byteOffset, byteSize).copyFrom(other);
    }
}
