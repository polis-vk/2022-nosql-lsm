package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class FileWorker {
    private static final long NOT_FOUND = -1;
    private static final long WRONG_SIZE = -1;
    private static final MemorySegment ALTERNATIVE = MemorySegment.ofArray(new byte[]{});

    private MemorySegment entries;
    private MemorySegment offsets;
    private Path pathToEntries;
    private Path pathToOffsets;
    private Status status;

    public void writeEntries(Collection<BaseEntry<MemorySegment>> data, Path basePath) throws IOException {
        long fileSize = data.stream()
                .mapToLong(entry -> entry.key().byteSize()
                        + (entry.value() == null ? Byte.BYTES : entry.value().byteSize()))
                .sum() + 2L * Long.BYTES * data.size();

        long count = createFiles(basePath);
        pathToEntries = basePath.resolve(FileName.SAVED_DATA.getName() + count);
        pathToOffsets = basePath.resolve(FileName.OFFSETS.getName() + count);

        try (ResourceScope scopeEntries = ResourceScope.newConfinedScope();
             ResourceScope scopeOffsets = ResourceScope.newConfinedScope()) {
            entries = createMappedSegment(pathToEntries, fileSize,
                    FileChannel.MapMode.READ_WRITE, scopeEntries);
            offsets = createMappedSegment(pathToOffsets, 3L * Long.BYTES * data.size() + 1,
                    FileChannel.MapMode.READ_WRITE, scopeOffsets);

            long index = 0L;
            long offset = 0L;
            for (BaseEntry<MemorySegment> entry : data) {
                long keySize = entry.key().byteSize();
                long valueSize = entry.value() == null ? WRONG_SIZE : entry.value().byteSize();

                MemoryAccess.setLongAtIndex(offsets, index++, offset);
                MemoryAccess.setLongAtIndex(offsets, index++, keySize);
                MemoryAccess.setLongAtIndex(offsets, index++, valueSize);

                entries.asSlice(offset).copyFrom(entry.key());
                offset += keySize;
                entries.asSlice(offset).copyFrom(valueSize == WRONG_SIZE
                        ? ALTERNATIVE : entry.value());
                offset += valueSize == WRONG_SIZE ? ALTERNATIVE.byteSize() : valueSize;
            }
        }
    }

    public BaseEntry<MemorySegment> findEntry(MemorySegment key, Path basePath) throws IOException {
        long fileCount = getFileCount(basePath);
        if (fileCount == 0 || key == null) {
            return null;
        }

        for (long i = fileCount - 1; i >= 0; i--) {
            createCurrentFiles(basePath, i);
            long result = binarySearch(key,
                    Files.size(pathToOffsets) / (Long.BYTES * 3) - 1,
                    SearchMode.SPECIFIC);

            if (result != NOT_FOUND) {
                BaseEntry<MemorySegment> found;
                try {
                    found = new BaseEntry<>(key, entries.asSlice(
                            MemoryAccess.getLongAtIndex(offsets, result * 3) + key.byteSize(),
                            MemoryAccess.getLongAtIndex(offsets, result * 3 + 2)
                    ));
                } catch (IndexOutOfBoundsException e) {
                    return null;
                }
                return found;
            }
        }
        return null;
    }

    public List<Iterator<BaseEntry<MemorySegment>>> findEntries(MemorySegment from,
                                                     MemorySegment to,
                                                     Path basePath) throws IOException {
        long count = getFileCount(basePath) - 1;
        if (count == -1) {
            return new ArrayList<>();
        }

        List<Iterator<BaseEntry<MemorySegment>>> iterators = new ArrayList<>();
        for (long i = count; i >= 0; i--) {
            createCurrentFiles(basePath, i);

            long boarder = Files.size(pathToOffsets) / (Long.BYTES * 3) - 1;
            long start = binarySearch(from, boarder, SearchMode.FROM);
            if (status == Status.LOWER) {
                if (start == boarder) {
                    iterators.add(Collections.emptyIterator());
                    continue;
                }
                start++;
            }

            long end = binarySearch(to, boarder, SearchMode.TO);
            if (status == Status.EQUALS || status == Status.HIGHER) {
                if (end == 0) {
                    iterators.add(Collections.emptyIterator());
                    continue;
                }
                end--;
            }

            if (start > end) {
                iterators.add(Collections.emptyIterator());
                continue;
            }
            iterators.add(new FileIterator(entries, offsets, start, end));
        }
        return iterators;
    }

    private long binarySearch(MemorySegment key, long boarder, SearchMode mode) {
        if (key == null) {
            status = Status.BOARDER;
            return mode == SearchMode.FROM ? 0 : boarder;
        }

        long mid = 0L;
        long left = 0L;
        long right = boarder;
        MemorySegment currentKey;

        while (left <= right) {
            mid = (left + right) / 2;
            currentKey = entries.asSlice(MemoryAccess.getLongAtIndex(offsets, mid * 3),
                    MemoryAccess.getLongAtIndex(offsets, mid * 3 + 1));
            int result = SegmentsComparator.compare(currentKey, key);
            if (result < 0) {
                status = Status.LOWER;
                left = mid + 1;
            } else if (result > 0) {
                status = Status.HIGHER;
                right = mid - 1;
            } else {
                status = Status.EQUALS;
                return mid;
            }
        }
        return mode == SearchMode.SPECIFIC ? NOT_FOUND : mid;
    }

    private MemorySegment createMappedSegment(Path path, long size,
                                              FileChannel.MapMode mapMode, ResourceScope scope) throws IOException {
        return MemorySegment.mapFile(
                path,
                0,
                size,
                mapMode,
                scope
        );
    }

    private long createFiles(Path basePath) throws IOException {
        long count = getFileCount(basePath);
        Files.createFile(basePath.resolve(FileName.SAVED_DATA.getName() + count));
        Files.createFile(basePath.resolve(FileName.OFFSETS.getName() + count));
        return count;
    }

    private void createCurrentFiles(Path basePath, long i) throws IOException {
        pathToEntries = basePath.resolve(FileName.SAVED_DATA.getName() + i);
        pathToOffsets = basePath.resolve(FileName.OFFSETS.getName() + i);
        entries = createMappedSegment(pathToEntries, Files.size(pathToEntries),
                FileChannel.MapMode.READ_ONLY, ResourceScope.newConfinedScope());
        offsets = createMappedSegment(pathToOffsets, Files.size(pathToOffsets),
                FileChannel.MapMode.READ_ONLY, ResourceScope.newConfinedScope());

    }

    private long getFileCount(Path basePath) {
        long count = 0;
        while (Files.exists(basePath.resolve(FileName.SAVED_DATA.getName() + count))) {
            count++;
        }
        return count;
    }
}
