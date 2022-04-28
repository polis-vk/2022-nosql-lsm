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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class FileWorker {
    private static final long WRONG_SIZE = -1;
    private static final String IS_COMPACT = "compact";
    private static final int LOWEST_PRIORITY = 0;
    private static final String CMP_DIR = "COMPACT DIRECTORY";
    private static final Logger log = Logger.getLogger(FileWorker.class.getName());

    private List<BaseEntry<MemorySegment>> files;
    private Path[] paths;

    public void load(Path basePath) {
        try {
            paths = getPaths(basePath);
            files = getMappedFiles();
        } catch (IOException e) {
            log.log(Level.SEVERE, "Broken files. Exception: ", e);
        }
    }

    public void writeEntries(Collection<BaseEntry<MemorySegment>> data,
                             Path basePath, boolean compacted, FileName... names) throws IOException {
        if (names.length != 2) {
            throw new IllegalArgumentException();
        }

        long fileSize = 0;
        for (BaseEntry<MemorySegment> entry : data) {
            fileSize += entry.key().byteSize();
            if (entry.value() != null) {
                fileSize += entry.value().byteSize();
                continue;
            }
            fileSize += Long.BYTES;
        }

        long nanos = createFiles(basePath, names);
        Path pathToEntries = basePath.resolve(names[0].getName() + nanos);
        Path pathToOffsets = basePath.resolve(names[1].getName() + nanos);

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
        if (compacted) {
            paths = getPathArray(basePath);
            if (paths[0].toString().contains(IS_COMPACT)) {
                CollapseTogether.moveFile(paths[0],
                        basePath.resolve(FileName.OFFSETS.getName() + LOWEST_PRIORITY));
                CollapseTogether.moveFile(paths[1],
                        basePath.resolve(FileName.SAVED_DATA.getName() + LOWEST_PRIORITY));

                paths = getPathArray(basePath);
            }
            files = getMappedFiles();
        }
    }

    public BaseEntry<MemorySegment> findEntry(MemorySegment key) throws IOException {
        if (paths.length == 0 || key == null) {
            return null;
        }

        int count = paths.length / 2;
        for (int i = count - 1; i >= 0; i--) {
            MemorySegment offsets = files.get(i).key();
            MemorySegment entries = files.get(i).value();

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

    public List<PeekIterator> findEntries(MemorySegment from, MemorySegment to) throws IOException {
        if (paths.length == 0) {
            return new ArrayList<>();
        }

        List<PeekIterator> iterator = new ArrayList<>();
        int count = paths.length / 2;

        for (int i = count - 1; i >= 0; i--) {
            MemorySegment offsets = files.get(i).key();
            MemorySegment entries = files.get(i).value();

            long boarder = Files.size(paths[i]) / (Long.BYTES * 3) - 1;
            long start = from == null ? 0 : Math.abs(binarySearch(from, offsets, entries, boarder));
            long end = to == null ? boarder : Math.abs(binarySearch(to, offsets, entries, boarder)) - 1;

            if (start > end) {
                continue;
            }

            iterator.add(new PeekIterator(new FileIterator(entries, offsets, start, end), i));
        }
        return iterator;
    }

    public long fileCount() {
        return paths.length / 2;
    }

    private Path[] getPaths(Path basePath) throws IOException {
        Path[] result = getPathArray(basePath);
        if (checkCompact(result)) {
            return recovery(result, basePath);
        }
        return result;
    }

    private Path[] getPathArray(Path basePath) throws IOException {
        try (Stream<Path> str = Files.list(basePath)) {
             return str.filter(i -> !Files.isDirectory(i))
                    .sorted(java.util.Comparator.comparing(Path::toString))
                    .toArray(Path[]::new);
        }
    }

    private List<BaseEntry<MemorySegment>> getMappedFiles() throws IOException {
        List<BaseEntry<MemorySegment>> mapped = new ArrayList<>();
        int count = paths.length / 2;
        for (int i = 0; i < count; i++) {
            MemorySegment offsets = createMappedSegment(paths[i], Files.size(paths[i]),
                    FileChannel.MapMode.READ_ONLY, ResourceScope.newConfinedScope());
            MemorySegment entries = createMappedSegment(paths[i + count], Files.size(paths[i + count]),
                    FileChannel.MapMode.READ_ONLY, ResourceScope.newConfinedScope());
            mapped.add(new BaseEntry<>(offsets, entries));
        }
        return mapped;
    }

    private boolean checkCompact(Path[] result) {
        return result.length > 1 && result[0].toString().contains(IS_COMPACT);
    }

    private Path[] recovery(Path[] result, Path basePath) throws IOException {
        CollapseTogether.removeOld(basePath);
        Path newOffsets = basePath.resolve(FileName.OFFSETS.getName() + LOWEST_PRIORITY);
        Path newEntries = basePath.resolve(FileName.SAVED_DATA.getName() + LOWEST_PRIORITY);
        Files.createFile(newOffsets);
        Files.createFile(newEntries);

        if (Files.size(result[0]) == 0) {
            result[0] = basePath.resolve(CMP_DIR).resolve(FileName.COMPACT_OFFSETS.getName());
        }

        if (!result[1].toString().contains(IS_COMPACT) || Files.size(result[1]) == 0) {
            result[1] = basePath.resolve(CMP_DIR).resolve(FileName.COMPACT_SAVED.getName());
        }

        CollapseTogether.moveFile(result[0], newOffsets);
        CollapseTogether.moveFile(result[1], newEntries);

        return new Path[] {newOffsets, newEntries};
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

    private long createFiles(Path basePath, FileName[] names) throws IOException {
        long nano = names[0] == FileName.COMPACT_SAVED ? LOWEST_PRIORITY : System.nanoTime();
        Files.createFile(basePath.resolve(names[0].getName() + nano));
        Files.createFile(basePath.resolve(names[1].getName() + nano));
        return nano;
    }
}
