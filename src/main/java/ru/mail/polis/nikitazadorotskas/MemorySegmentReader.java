package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

class MemorySegmentReader {
    private MemorySegment mappedSegmentForIndexes;
    private MemorySegment mappedSegmentForData;
    private final Utils utils;
    private final ResourceScope scope;
    private long lastIndex;

    MemorySegmentReader(Utils utils, ResourceScope scope) throws IOException {
        this.utils = utils;
        this.scope = scope;
        createMappedForData();
        createMappedForIndexes();
    }

    BaseEntry<MemorySegment> getFromDisk(MemorySegment key) {
        return binarySearch(key);
    }

    private void createMappedForIndexes() throws IOException {
        long fileSize = Files.size(utils.getIndexesPath());
        lastIndex = fileSize / Long.BYTES - 3;
        mappedSegmentForIndexes = createMappedSegment(utils.getIndexesPath(), fileSize);
    }

    private void createMappedForData() throws IOException {
        long fileSize = Files.size(utils.getStoragePath());
        mappedSegmentForData = createMappedSegment(utils.getStoragePath(), fileSize);
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

    private BaseEntry<MemorySegment> binarySearch(MemorySegment key) {
        long low = 0;
        long high = lastIndex;

        while (low < high) {
            long mid = countMid(low, high);

            MemorySegment currentKey = getMemorySegment(mid);
            int compare = utils.compareMemorySegment(key, currentKey);

            if (compare > 0) {
                low = mid + 2;
            } else if (compare == 0) {
                return new BaseEntry<>(currentKey, getMemorySegment(mid + 1));
            } else {
                high = mid;
            }
        }

        MemorySegment currentMemorySegment = getMemorySegment(low);

        if (utils.compareMemorySegment(key, currentMemorySegment) == 0) {
            return new BaseEntry<>(currentMemorySegment, getMemorySegment(low + 1));
        }

        return null;
    }

    private long countMid(long low, long high) {
        long mid = low + ((high - low) / 2); // Аналогично (low + high) / 2, но так не будет переполнения
        if (mid % 2 == 1) { // Делаю так, потому что по нечетным индексам находятся значения, а не ключи
            mid--;
        }
        return mid;
    }

    private MemorySegment getMemorySegment(long index) {
        long byteOffset = MemoryAccess.getLongAtIndex(mappedSegmentForIndexes, index);
        long byteSize = MemoryAccess.getLongAtIndex(mappedSegmentForIndexes, index + 1) - byteOffset;
        return mappedSegmentForData.asSlice(byteOffset, byteSize);
    }
}
