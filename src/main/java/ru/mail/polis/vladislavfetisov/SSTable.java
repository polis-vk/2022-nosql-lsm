package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public final class SSTable {
    public static final int NULL_VALUE = -1;
    public static final String TEMP = "_tmp";
    public static final String INDEX = "_i";
    private final MemorySegment mapFile;
    private final MemorySegment mapIndex;
    private final Path tableName;
    private final Path indexName;

    public Path getTableName() {
        return tableName;
    }

    public Path getIndexName() {
        return indexName;
    }

    private SSTable(Path tableName, Path indexName) throws IOException {
        mapFile = Utils.map(tableName, Files.size(tableName), FileChannel.MapMode.READ_ONLY);
        this.tableName = tableName;
        mapIndex = Utils.map(indexName, Files.size(indexName), FileChannel.MapMode.READ_ONLY);
        this.indexName = indexName;
    }

    public static List<SSTable> getAllTables(Path dir) {
        try (Stream<Path> files = Files.list(dir)) {
            return files
                    .filter(path -> !path.toString().endsWith(INDEX))
                    .mapToInt(path -> Integer.parseInt(path.getFileName().toString()))
                    .sorted()
                    .mapToObj(i -> mapToTable(dir.resolve(String.valueOf(i))))
                    .toList();
        } catch (IOException e) {
            return Collections.emptyList();
        }
    }

    private static SSTable mapToTable(Path path) {
        try {
            Path index = Utils.withSuffix(path, INDEX);
            return new SSTable(path, index);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static SSTable writeTable(Path table, Collection<Entry<MemorySegment>> values) throws IOException {
        Path tableTemp = Utils.withSuffix(table, TEMP);

        Path index = table.resolveSibling(table + INDEX);
        Path indexTemp = Utils.withSuffix(index, TEMP);

        long tableSize = 0;
        for (Entry<MemorySegment> entry : values) {
            tableSize += Utils.sizeOfEntry(entry);
        }
        long indexSize = (long) values.size() * Long.BYTES;

        newFile(tableTemp);
        newFile(indexTemp);

        MemorySegment fileMap = Utils.map(tableTemp, tableSize, FileChannel.MapMode.READ_WRITE);
        MemorySegment indexMap = Utils.map(indexTemp, indexSize, FileChannel.MapMode.READ_WRITE);

        long indexOffset = 0;
        long fileOffset = 0;

        for (Entry<MemorySegment> entry : values) {
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

        return new SSTable(table, index);
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
}
