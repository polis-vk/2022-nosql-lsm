package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.file.Files;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

class MemorySegmentReader {
    private long[] indexes;
    MemorySegment mappedSegment;
    Utils utils;

    MemorySegmentReader(Utils utils) {
        this.utils = utils;
    }

    BaseEntry<MemorySegment> getFromDisk(MemorySegment key) {
        if (filesDoesNotExist()) {
            return null;
        }

        if (indexes == null) {
            readIndexes();
        }

        try {
            if (mappedSegment == null) {
                createdMapped();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return binarySearch(key);
    }

    private boolean filesDoesNotExist() {
        return !Files.exists(utils.getIndexesPath()) || !Files.exists(utils.getStoragePath());
    }

    private void readIndexes() {
        try {
            long fileSize = Files.size(utils.getIndexesPath());
            MemorySegment ms = MemorySegment.mapFile(
                    utils.getIndexesPath(),
                    0,
                    fileSize,
                    READ_ONLY,
                    ResourceScope.globalScope()
            );
            indexes = ms.toLongArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createdMapped() throws IOException {
        long fileSize = Files.size(utils.getStoragePath());
        mappedSegment = MemorySegment.mapFile(
                utils.getStoragePath(),
                0,
                fileSize,
                READ_WRITE,
                ResourceScope.globalScope()
        );
    }

    private BaseEntry<MemorySegment> binarySearch(MemorySegment key) {
        int low = 0;
        int high = indexes.length - 3;

        while (low < high) {
            int mid = countMid(low, high);

            MemorySegment currentKey = getMemorySegment(mid);
            int compare = Utils.compareMemorySegment(key, currentKey);

            if (compare > 0) {
                low = mid + 2;
            } else if (compare == 0) {
                return new BaseEntry<>(currentKey, getMemorySegment(mid + 1));
            } else {
                high = mid;
            }
        }

        MemorySegment currentMemorySegment = getMemorySegment(low);

        if (Utils.compareMemorySegment(key, currentMemorySegment) == 0) {
            return new BaseEntry<>(currentMemorySegment, getMemorySegment(low + 1));
        }

        return null;
    }

    private int countMid(int low, int high) {
        int mid = low + ((high - low) / 2);
        if (mid % 2 == 1) {
            mid--;
        }
        return mid;
    }

    private MemorySegment getMemorySegment(int index) {
        long byteOffset = indexes[index];
        long byteSize = indexes[index + 1] - byteOffset;
        return mappedSegment.asSlice(byteOffset, byteSize);
    }
}
