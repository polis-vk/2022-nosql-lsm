package ru.mail.polis.vladislavfetisov.lsm;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Entry;
import ru.mail.polis.vladislavfetisov.MemorySegments;
import ru.mail.polis.vladislavfetisov.Utils;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.Cleaner;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SSTable implements Closeable {
    public static final String TEMP = "_tmp";
    public static final String INDEX = "_i";
    public static final String COMPACTED = "_compacted";
    private final MemorySegment mapFile;
    private final MemorySegment mapIndex;
    private final Path tableName;
    private final Path indexName;
    private final ResourceScope sharedScope;

    private static final Cleaner cleaner = Cleaner.create(r -> {
        Thread cleanerThread = new Thread(r, "Cleaner thread");
        cleanerThread.setDaemon(true);
        return cleanerThread;
    });

    public Path getIndexName() {
        return indexName;
    }

    public Path getTableName() {
        return tableName;
    }

    private SSTable(Path tableName, Path indexName, long tableSize, long indexSize) throws IOException {
        sharedScope = ResourceScope.newSharedScope(cleaner);
        mapFile = MemorySegments.map(tableName, tableSize, FileChannel.MapMode.READ_ONLY, sharedScope);
        this.tableName = tableName;
        mapIndex = MemorySegments.map(indexName, indexSize, FileChannel.MapMode.READ_ONLY, sharedScope);
        this.indexName = indexName;
    }

    public static Directory retrieveDir(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            Set<Path> compactedTables = new HashSet<>();
            List<Path> paths = sortPathsAndFindCompacted(dir, files, compactedTables);

            int indexOfLastCompacted = lastCompactedIndex(compactedTables, paths);

            List<SSTable> ssTables = paths
                    .stream()
                    .map(SSTable::mapToTable)
                    .toList();
            return new Directory(ssTables, indexOfLastCompacted);
        }
    }

    private static List<Path> sortPathsAndFindCompacted(Path dir, Stream<Path> files, Set<Path> compactedTables) {
        return files
                .filter(path -> {
                    String s = path.toString();
                    return !(s.endsWith(INDEX) || s.endsWith(TEMP));
                })
                .mapToInt(path -> processFileName(compactedTables, path))
                .sorted()
                .mapToObj(i -> dir.resolve(String.valueOf(i)))
                .collect(Collectors.toList());
    }

    private static int processFileName(Set<Path> compactedTables, Path path) {
        if (path.toString().endsWith(COMPACTED)) {
            Path removedSuffix = Path.of(Utils.removeSuffix(path.toString(), COMPACTED));
            compactedTables.add(removedSuffix);
            return Utils.getTableNum(removedSuffix);
        }
        return Utils.getTableNum(path);
    }

    private static int lastCompactedIndex(Set<Path> compactedTables, List<Path> paths) {
        int lastCompactedIndex = 0;
        if (compactedTables.isEmpty()) {
            return 0;
        }
        if (compactedTables.size() > 2) {
            throw new IllegalStateException("compactedTables: " + compactedTables.size());
        }
        for (int i = 0; i < paths.size(); i++) {
            Path path = paths.get(i);
            if (compactedTables.contains(path)) {
                lastCompactedIndex = i;
                paths.set(i, Utils.withSuffix(path, COMPACTED));
            }
        }
        return lastCompactedIndex;
    }

    private static SSTable mapToTable(Path path) {
        try {
            Path index = Utils.withSuffix(path, INDEX);
            return new SSTable(path, index, Files.size(path), Files.size(index));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static SSTable writeTable(Path table,
                                     Iterator<Entry<MemorySegment>> values,
                                     long tableSize,
                                     long indexSize) throws IOException {
        Path tableTemp = Utils.withSuffix(table, TEMP);

        Path index = table.resolveSibling(table + INDEX);
        Path indexTemp = Utils.withSuffix(index, TEMP);

        Utils.newFile(tableTemp);
        Utils.newFile(indexTemp);

        try (ResourceScope writingScope = ResourceScope.newSharedScope()) {
            MemorySegment fileMap = MemorySegments.map(tableTemp, tableSize, FileChannel.MapMode.READ_WRITE, writingScope);
            MemorySegment indexMap = MemorySegments.map(indexTemp, indexSize, FileChannel.MapMode.READ_WRITE, writingScope);

            long indexOffset = 0;
            long fileOffset = 0;

            while (values.hasNext()) {
                Entry<MemorySegment> entry = values.next();
                MemoryAccess.setLongAtOffset(indexMap, indexOffset, fileOffset);
                indexOffset += Long.BYTES;

                fileOffset = MemorySegments.writeEntry(fileMap, fileOffset, entry);
            }
            fileMap.force();
            indexMap.force();
            Utils.rename(indexTemp, index);
            Utils.rename(tableTemp, table);
        }
        return new SSTable(table, index, tableSize, indexSize);
    }

    public Iterator<Entry<MemorySegment>> range(MemorySegment from, MemorySegment to) {
        long li = 0;
        long ri = mapIndex.byteSize() / Long.BYTES;
        if (from != null) {
            li = Utils.binarySearch(from, mapFile, mapIndex);
            if (li == -1) {
                li = 0;
            }
            if (li == ri) {
                return Collections.emptyIterator();
            }
        }

        if (to != null) {
            ri = Utils.binarySearch(to, mapFile, mapIndex);
            if (ri == -1) {
                return Collections.emptyIterator();
            }
        }

        long finalLi = li;
        long finalRi = ri;
        return new Iterator<>() {
            long pos = finalLi;

            @Override
            public boolean hasNext() {
                return pos < finalRi;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Entry<MemorySegment> res = MemorySegments.getByIndex(mapFile, mapIndex, pos);
                pos++;
                return res;
            }
        };
    }

    @Override
    public void close() throws IOException {
        sharedScope.close();
    }

    public boolean isCompacted() {
        return tableName.toString().endsWith(COMPACTED);
    }

    /**
     * record Sizes contains tableSize-size of SSTable,
     * indexSize-size of indexTable.
     */
    public record Sizes(long tableSize, long indexSize) {
        //empty
    }

    public record Directory(List<SSTable> ssTables, int indexOfLastCompacted) {
        //empty
    }

}
