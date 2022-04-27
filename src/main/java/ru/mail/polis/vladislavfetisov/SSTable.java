package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import static ru.mail.polis.vladislavfetisov.LsmDao.logger;

public final class SSTable implements Closeable {
    public static final int NULL_VALUE = -1;
    public static final String TEMP = "_tmp";
    public static final String INDEX = "_i";
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

    private SSTable(Path tableName, Path indexName, long tableSize, long indexSize) throws IOException {
        sharedScope = ResourceScope.newSharedScope();
        mapFile = Utils.map(tableName, tableSize, FileChannel.MapMode.READ_ONLY, sharedScope);
        this.tableName = tableName;
        mapIndex = Utils.map(indexName, indexSize, FileChannel.MapMode.READ_ONLY, sharedScope);
        this.indexName = indexName;
    }

    public static List<SSTable> getAllTables(Path dir) {
        try (Stream<Path> files = Files.list(dir)) {
            return files
                    .filter(path -> {
                        String s = path.toString();
                        return !(s.endsWith(INDEX) || s.endsWith(TEMP));
                    })
                    .mapToInt(path -> Integer.parseInt(path.getFileName().toString()))
                    .sorted()
                    .mapToObj(i -> mapToTable(dir.resolve(String.valueOf(i))))
                    .toList();
        } catch (IOException e) {
            logger.info("No SSTables in directory");
            return Collections.emptyList();
        }
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

    /**
     * record Sizes contains tableSize-size of SSTable,
     * indexSize-size of indexTable.
     */
    public record Sizes(long tableSize, long indexSize) {
        //empty
    }
}
