package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public final class SSTable {
    public static final String TEMP = "_tmp";
    public static final String INDEX = "_i";
    private final MemorySegment mapFile;
    private final MemorySegment mapIndex;

    private SSTable(Path tableName, Path indexName) throws IOException {
        mapFile = Utils.map(tableName, Files.size(tableName), FileChannel.MapMode.READ_ONLY);
        mapIndex = Utils.map(indexName, Files.size(indexName), FileChannel.MapMode.READ_ONLY);
    }

    public static List<SSTable> getAllTables(Path dir) {
        try (Stream<Path> files = Files.list(dir)) {
            return files
                    .filter(path -> !path.toString().endsWith(INDEX))
                    .sorted((p1, p2) -> {
                        int first = Integer.parseInt(p1.getFileName().toString());
                        int second = Integer.parseInt(p2.getFileName().toString());
                        return Integer.compare(first, second);
                    })
                    .map(SSTable::mapToTable)
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

        Files.createFile(tableTemp);
        Files.createFile(indexTemp);
        MemorySegment fileMap = Utils.map(tableTemp, tableSize, FileChannel.MapMode.READ_WRITE);
        MemorySegment indexMap = Utils.map(indexTemp, indexSize, FileChannel.MapMode.READ_WRITE);

        long indexOffset = 0;
        long fileOffset = 0;

        for (Entry<MemorySegment> entry : values) {
            MemoryAccess.setLongAtOffset(indexMap, indexOffset, fileOffset);
            indexOffset += Long.BYTES;

            fileOffset += Utils.writeSegment(entry.key(), fileMap, fileOffset);

            if (entry.value() == null) {
                MemoryAccess.setLongAtOffset(fileMap, fileOffset, -1);
                fileOffset += Long.BYTES;
                continue;
            }
            fileOffset += Utils.writeSegment(entry.value(), fileMap, fileOffset);
        }
        Utils.rename(tableTemp, table);
        Utils.rename(indexTemp, index);

        return new SSTable(table, index);
    }

    public Iterator<Entry<MemorySegment>> range(MemorySegment from, MemorySegment to) {
        MemorySegment readOnlyFile = mapFile.asReadOnly();
        MemorySegment readOnlyIndex = mapIndex.asReadOnly();
        long li = 0;
        if (from != null) {
            li = Utils.binarySearch(from, readOnlyFile, readOnlyIndex);
            if (li == -1) {
                li = 0;
            }
            if (li < -1) {
                return Collections.emptyIterator();
            }

        }
        long ri = readOnlyIndex.byteSize() / Long.BYTES;
        if (to != null) {
            ri = Utils.binarySearch(to, readOnlyFile, readOnlyIndex);
            if (ri == -1) {
                return Collections.emptyIterator();
            }
            if (ri < -1) {
                ri = readOnlyIndex.byteSize() / Long.BYTES;
            }
        }

        long finalLi = li;
        long finalRi = ri;
        return new Iterator<>() {
            long left = finalLi;

            @Override
            public boolean hasNext() {
                return left < finalRi;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Entry<MemorySegment> res = Utils.getByIndex(readOnlyFile, readOnlyIndex, left);
                left++;
                return res;
            }
        };
    }
}