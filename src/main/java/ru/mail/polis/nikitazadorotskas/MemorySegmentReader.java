package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

class MemorySegmentReader {
    private final long dataOffset;
    private final long lastIndex;
    private final int number;
    private final Utils utils;
    private final ResourceScope scope;
    private final MemorySegment mappedSegment;
    private long lastIndexFoundInBinarySearch;

    MemorySegmentReader(Utils utils, ResourceScope scope, int number) throws IOException {
        this.utils = utils;
        this.scope = scope;
        this.number = number;

        this.mappedSegment = createMapped();

        long numberOfEntries = MemoryAccess.getLongAtIndex(mappedSegment, 0);
        this.lastIndex = numberOfEntries / 2 - 1;
        this.dataOffset = numberOfEntries * Long.BYTES + Long.BYTES;
    }

    private MemorySegment createMapped() throws IOException {
        long fileSize = Files.size(utils.getStoragePath(number));
        return createMappedSegment(utils.getStoragePath(number), fileSize);
    }

    private MemorySegment createMappedSegment(Path path, long size) throws IOException {
        return MemorySegment.mapFile(
                path,
                0,
                size,
                READ_ONLY,
                scope
        );
    }

    BaseEntry<MemorySegment> getFromDisk(MemorySegment key) {
        return binarySearch(key);
    }

    private BaseEntry<MemorySegment> binarySearch(MemorySegment key) {
        long low = 0;
        long high = lastIndex;

        while (low <= high) {
            long mid = countMid(low, high);

            MemorySegment currentKey = getMemorySegment(mid, false);
            int compare = utils.compareMemorySegment(key, currentKey);

            if (compare > 0) {
                low = mid + 1;
            } else if (compare == 0) {
                lastIndexFoundInBinarySearch = mid;
                return new BaseEntry<>(currentKey, getMemorySegment(mid, true));
            } else {
                high = mid - 1;
            }
        }

        lastIndexFoundInBinarySearch = low;
        return null;
    }

    private long countMid(long low, long high) {
        return (low + high) >>> 1;
    }

    private MemorySegment getMemorySegment(long entityIndex, boolean isValue) {
        long segmentIndex = entityIndex * 2;
        if (isValue) {
            segmentIndex++;
        }

        long nextOffset = MemoryAccess.getLongAtIndex(mappedSegment, segmentIndex + 2);
        if (nextOffset == -1) {
            return null;
        }

        long byteOffset = getByteOffset(segmentIndex);
        long byteSize = nextOffset - byteOffset;

        return mappedSegment.asSlice(dataOffset + byteOffset, byteSize);
    }

    private long getByteOffset(long segmentIndex) {
        long byteOffset = MemoryAccess.getLongAtIndex(mappedSegment, segmentIndex + 1);

        if (byteOffset == -1) {
            byteOffset = MemoryAccess.getLongAtIndex(mappedSegment, segmentIndex);
        }

        return byteOffset;
    }

    public PeekIterator getFromDisk(MemorySegment from, MemorySegment to) {
        long start = getIndex(from, 0);
        long end = getIndex(to, lastIndex + 1);

        if (end < start) {
            throw new IllegalArgumentException("from is bigger than to");
        }

        return new PeekIterator(number, new Iterator<>() {
            long next = start;

            @Override
            public boolean hasNext() {
                return next < end;
            }

            @Override
            public BaseEntry<MemorySegment> next() {
                return new BaseEntry<>(getMemorySegment(next, false), getMemorySegment(next++, true));
            }
        });
    }

    private long getIndex(MemorySegment key, long defaultIndex) {
        if (key == null) {
            return defaultIndex;
        }

        binarySearch(key);
        return lastIndexFoundInBinarySearch;
    }

    public int getNumber() {
        return number;
    }
}
