package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public final class MemorySegments {
    public static final int NULL_VALUE = -1;

    private MemorySegments() {

    }

    public static int compareMemorySegments(MemorySegment o1, MemorySegment o2) {
        long mismatch = o1.mismatch(o2);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == o1.byteSize()) {
            return -1;
        }
        if (mismatch == o2.byteSize()) {
            return 1;
        }
        byte b1 = MemoryAccess.getByteAtOffset(o1, mismatch);
        byte b2 = MemoryAccess.getByteAtOffset(o2, mismatch);
        return Byte.compare(b1, b2);
    }

    public static Entry<MemorySegment> getByIndex(MemorySegment mapFile, MemorySegment mapIndex, long index) {
        long offset = getLength(mapIndex, index * Long.BYTES);

        return readEntryByOffset(mapFile, offset);
    }

    public static Entry<MemorySegment> readEntryByOffset(MemorySegment mapFile, long offsetValue) {
        long offset = offsetValue;
        long keyLength = getLength(mapFile, offset);
        offset += Long.BYTES;
        MemorySegment key = mapFile.asSlice(offset, keyLength);

        offset += keyLength;
        long valueLength = getLength(mapFile, offset);
        MemorySegment value;
        if (valueLength == NULL_VALUE) {
            value = null;
        } else {
            value = mapFile.asSlice(offset + Long.BYTES, valueLength);
        }
        return new BaseEntry<>(key, value);
    }

    private static long getLength(MemorySegment mapFile, long offset) {
        return MemoryAccess.getLongAtOffset(mapFile, offset);
    }

    public static MemorySegment map(Path table,
                                    long length,
                                    FileChannel.MapMode mapMode,
                                    ResourceScope scope) throws IOException {
        return MemorySegment.mapFile(table, 0, length, mapMode, scope);
    }

    public static long writeEntry(MemorySegment fileMap, long offset, Entry<MemorySegment> entry) {
        long fileOffset = offset;
        fileOffset += writeSegment(entry.key(), fileMap, fileOffset);

        if (entry.value() == null) {
            writeLength(fileMap, fileOffset, NULL_VALUE);
            return fileOffset + Long.BYTES;
        }
        fileOffset += writeSegment(entry.value(), fileMap, fileOffset);
        return fileOffset;
    }

    private static long writeSegment(MemorySegment segment, MemorySegment fileMap, long fileOffset) {
        long length = segment.byteSize();
        writeLength(fileMap, fileOffset, length);

        fileMap.asSlice(fileOffset + Long.BYTES).copyFrom(segment);

        return Long.BYTES + length;
    }

    public static void writeLength(MemorySegment fileMap, long fileOffset, long value) {
        MemoryAccess.setLongAtOffset(fileMap, fileOffset, value);
    }
}
