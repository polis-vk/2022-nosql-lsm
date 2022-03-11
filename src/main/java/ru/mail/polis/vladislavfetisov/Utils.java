package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

public final class Utils {
    private Utils() {

    }

    public static long sizeOfEntry(Entry<MemorySegment> entry) {
        long valueSize = (entry.value() == null) ? 0 : entry.value().byteSize();
        return 2L * Long.BYTES + entry.key().byteSize() + valueSize;
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

    public static Entry<MemorySegment> binarySearch(MemorySegment key, MemorySegment mapFile, MemorySegment mapIndex) {
        long l = 0;
        long r = mapIndex.byteSize() / Long.BYTES;
        while (l <= r) {
            long middle = (l + r) >>> 1;
            Entry<MemorySegment> middleEntry = getByIndex(mapFile, mapIndex, middle);
            int res = compareMemorySegments(middleEntry.key(), key);
            if (res == 0) {
                return middleEntry;
            }
            if (res < 0) {
                l = middle + 1;
            } else {
                r = middle - 1;
            }
        }
        return null;
    }

    private static Entry<MemorySegment> getByIndex(MemorySegment mapFile, MemorySegment mapIndex, long index) {
        long offset = getLength(mapIndex, index * Long.BYTES);

        long keyLength = getLength(mapFile, offset);
        MemorySegment key = mapFile.asSlice(offset + Long.BYTES, keyLength);

        offset += Long.BYTES + keyLength;
        long valueLength = getLength(mapFile, offset);
        MemorySegment value;
        if (valueLength == -1) {
            value = null;
        } else {
            value = mapFile.asSlice(offset + Long.BYTES, valueLength);
        }
        return new BaseEntry<>(key, value);
    }

    private static long getLength(MemorySegment mapFile, long offset) {
        return MemoryAccess.getLongAtOffset(mapFile, offset);
    }

}
