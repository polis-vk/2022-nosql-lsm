package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.Cleaner;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SSTable implements Closeable {
    public static final int NULL_VALUE = -1;
    public static final String TEMP = "_tmp";
    public static final String INDEX = "_i";
    public static final String COMPACTED = "_compacted";
    private final MemorySegment mapFile;
    private final MemorySegment mapIndex;
    private final Path tableName;
    private final Path indexName;
    private final ResourceScope sharedScope;

    public Path getTableName() {
        return tableName;
    }

    public Path getIndexName() {
        return indexName;
    }

    private static final Cleaner cleaner = Cleaner.create(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "Cleaner thread") {
                @Override
                public synchronized void start() {
                    setDaemon(true);
                    super.start();
                }
            };
        }
    });

    private SSTable(Path tableName, Path indexName, long tableSize, long indexSize) throws IOException {
        sharedScope = ResourceScope.newSharedScope(cleaner);
        mapFile = Utils.map(tableName, tableSize, FileChannel.MapMode.READ_ONLY, sharedScope);
        this.tableName = tableName;
        mapIndex = Utils.map(indexName, indexSize, FileChannel.MapMode.READ_ONLY, sharedScope);
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
            return getTableNum(removedSuffix);
        }
        return getTableNum(path);
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

        newFile(tableTemp);
        newFile(indexTemp);

        try (ResourceScope writingScope = ResourceScope.newSharedScope()) {
            MemorySegment fileMap = Utils.map(tableTemp, tableSize, FileChannel.MapMode.READ_WRITE, writingScope);
            MemorySegment indexMap = Utils.map(indexTemp, indexSize, FileChannel.MapMode.READ_WRITE, writingScope);

            long indexOffset = 0;
            long fileOffset = 0;

            while (values.hasNext()) {
                Entry<MemorySegment> entry = values.next();
                MemoryAccess.setLongAtOffset(indexMap, indexOffset, fileOffset);
                indexOffset += Long.BYTES;

                fileOffset += Utils.writeSegment(entry.key(), fileMap, fileOffset);

                if (entry.value() == null) {
                    MemoryAccess.setLongAtOffset(fileMap, fileOffset, NULL_VALUE);
                    fileOffset += Long.BYTES;
                    continue;
                }
                fileOffset += Utils.writeSegment(entry.value(), fileMap, fileOffset);
            }
            Utils.rename(indexTemp, index);
            Utils.rename(tableTemp, table);
        }
        return new SSTable(table, index, tableSize, indexSize);
    }

    private static void newFile(Path tableTemp) throws IOException {
        Files.newByteChannel(tableTemp,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
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
                Entry<MemorySegment> res = Utils.getByIndex(mapFile, mapIndex, pos);
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

    public static Integer getTableNum(Path path) {
        return Integer.parseInt(path.getFileName().toString());
    }

}
