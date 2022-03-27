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
import java.util.List;
import java.util.stream.Stream;

public class FileWorker {
    private static final long WRONG_SIZE = -1;

    public void writeEntries(Collection<BaseEntry<MemorySegment>> data, Path basePath) throws IOException {
        long fileSize = 0;
        for (BaseEntry<MemorySegment> entry : data) {
            fileSize += entry.key().byteSize();
            if (entry.value() != null) {
                fileSize += entry.value().byteSize();
                continue;
            }
            fileSize += Long.BYTES;
        }

        long millis = createFiles(basePath);
        Path pathToEntries = basePath.resolve(FileName.SAVED_DATA.getName() + millis);
        Path pathToOffsets = basePath.resolve(FileName.OFFSETS.getName() + millis);

        try (ResourceScope scopeEntries = ResourceScope.newConfinedScope();
             ResourceScope scopeOffsets = ResourceScope.newConfinedScope()) {
            MemorySegment entries = createMappedSegment(pathToEntries, fileSize,
                    FileChannel.MapMode.READ_WRITE, scopeEntries);
            MemorySegment offsets = createMappedSegment(pathToOffsets, 3L * Long.BYTES * data.size() + 1,
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

                if (entry.value() != null) {
                    entries.asSlice(offset).copyFrom(entry.value());
                    offset += valueSize;
                }
            }
        }
    }

    public BaseEntry<MemorySegment> findEntry(MemorySegment key, Path basePath) throws IOException {
        Path[] paths = getPaths(basePath);
        if (paths.length == 0 || key == null) {
            return null;
        }

        int count = paths.length / 2;
        for (int i = count - 1; i >= 0; i--) {
            MemorySegment offsets = createMappedSegment(paths[i], Files.size(paths[i]),
                    FileChannel.MapMode.READ_ONLY, ResourceScope.newConfinedScope());
            MemorySegment entries = createMappedSegment(paths[i + count], Files.size(paths[i + count]),
                    FileChannel.MapMode.READ_ONLY, ResourceScope.newConfinedScope());

            long result = binarySearch(
                    key,
                    offsets,
                    entries,
                    Files.size(paths[i]) / (Long.BYTES * 3) - 1
            );

            if (result >= 0) {
                long valueSize = MemoryAccess.getLongAtIndex(offsets, result * 3 + 2);
                if (valueSize == WRONG_SIZE) {
                    return null;
                }

                MemorySegment currentKey = entries.asSlice(
                        MemoryAccess.getLongAtIndex(offsets, result * 3),
                        MemoryAccess.getLongAtIndex(offsets, result * 3 + 1)
                );

                if (Comparator.compare(currentKey, key) != 0) {
                    return null;
                }

                return new BaseEntry<>(key,
                        entries.asSlice(
                                MemoryAccess.getLongAtIndex(offsets, result * 3) + key.byteSize(),
                                valueSize
                        )
                );
            }
        }
        return null;
    }

    public List<PeekIterator> findEntries(MemorySegment from,
                                                     MemorySegment to,
                                                     Path basePath) throws IOException {
        Path[] paths = getPaths(basePath);
        if (paths.length == 0) {
            return new ArrayList<>();
        }

        List<PeekIterator> iterator = new ArrayList<>();
        int count = paths.length / 2;

        for (int i = count - 1; i >= 0; i--) {
            MemorySegment offsets = createMappedSegment(paths[i], Files.size(paths[i]),
                    FileChannel.MapMode.READ_ONLY, ResourceScope.newConfinedScope());
            MemorySegment entries = createMappedSegment(paths[i + count], Files.size(paths[i + count]),
                    FileChannel.MapMode.READ_ONLY, ResourceScope.newConfinedScope());

            long boarder = Files.size(paths[i]) / (Long.BYTES * 3) - 1;
            long start = from == null ? 0 : Math.abs(binarySearch(from, offsets, entries, boarder));
            long end = to == null ? boarder : Math.abs(binarySearch(to, offsets, entries, boarder)) - 1;

            if (start > end) {
                iterator.add(new PeekIterator(Collections.emptyIterator(), i));
                continue;
            }

            iterator.add(new PeekIterator(new FileIterator(entries, offsets, start, end), i));
        }
        return iterator;
    }

    private long binarySearch(MemorySegment key,
                              MemorySegment offsets,
                              MemorySegment entries,
                              long boarder) {
        long mid;
        long left = 0L;
        long right = boarder;
        MemorySegment currentKey;

        while (left <= right) {
            mid = (left + right) >>> 1;
            currentKey = entries.asSlice(MemoryAccess.getLongAtIndex(offsets, mid * 3),
                    MemoryAccess.getLongAtIndex(offsets, mid * 3 + 1));
            int result = Comparator.compare(currentKey, key);
            if (result < 0) {
                left = mid + 1;
            } else if (result > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }
        return -left;
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
        long nano = System.nanoTime();
        Files.createFile(basePath.resolve(FileName.SAVED_DATA.getName() + nano));
        Files.createFile(basePath.resolve(FileName.OFFSETS.getName() + nano));
        return nano;
    }

    private Path[] getPaths(Path basePath) throws IOException {
        try (Stream<Path> str = Files.list(basePath)) {
            return str.toArray(Path[]::new);
        }
    }
}
