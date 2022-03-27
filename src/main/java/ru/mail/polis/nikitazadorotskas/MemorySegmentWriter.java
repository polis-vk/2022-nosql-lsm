package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

class MemorySegmentWriter {
    private final long dataOffset;
    private final MemorySegment mappedMemorySegment;
    private final ResourceScope scope;
    private long lastSize;
    private long lastIndex;
    private int arrayIndex;

    MemorySegmentWriter(int arraySize, long storageSize, Utils utils, ResourceScope scope, int number)
            throws IOException {
        this.scope = scope;
        utils.createStorageFile(number);

        long numberOfEntriesByteSize = Long.BYTES;
        long numberOfEntries = arraySize * 2L + 1;
        long indexesSize = Long.BYTES * numberOfEntries;
        this.dataOffset = numberOfEntriesByteSize + indexesSize;

        // sizes:       1 long       numberOfEntries * 2 + 1 longs      storageSize
        // storage: ((numberOfEntries)(key0index, value0index...)(key0, value0...))
        this.mappedMemorySegment = createMappedSegment(
                utils.getStoragePath(number),
                dataOffset + storageSize);
        MemoryAccess.setLongAtIndex(mappedMemorySegment, 0, numberOfEntries);
    }

    private MemorySegment createMappedSegment(Path path, long size) throws IOException {
        return MemorySegment.mapFile(path, 0, size, FileChannel.MapMode.READ_WRITE, scope);
    }

    void writeEntry(BaseEntry<MemorySegment> entry) {
        writePartOfEntry(entry.key());
        writePartOfEntry(entry.value());
    }

    private void writePartOfEntry(MemorySegment data) {
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
        MemoryAccess.setLongAtIndex(mappedMemorySegment, ++arrayIndex + 1, index);
    }

    private void writeData(MemorySegment other) {
        writeToMappedMemorySegment(mappedMemorySegment, getCurrentOffset(), lastSize, other);
    }

    private long getCurrentOffset() {
        return dataOffset + lastIndex - lastSize;
    }

    private void writeToMappedMemorySegment(MemorySegment mapped, long byteOffset, long byteSize, MemorySegment other) {
        mapped.asSlice(byteOffset, byteSize).copyFrom(other);
    }
}
