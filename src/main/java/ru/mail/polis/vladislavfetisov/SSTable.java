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
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class SSTable {
    public static final String TEMP = "_tmp";
    public static final String INDEX = "_i";
    private final MemorySegment mapFile;
    private final MemorySegment mapIndex;
    private final Path tableName;
    private final Path indexName;

    private SSTable(Path tableName, Path indexName) throws IOException {
        this.tableName = tableName;
        this.indexName = indexName;

        mapFile = Utils.map(tableName, Files.size(tableName), FileChannel.MapMode.READ_ONLY);
        mapIndex = Utils.map(indexName, Files.size(indexName), FileChannel.MapMode.READ_ONLY);
    }

    public static List<SSTable> getAllSSTables(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files
                    .filter((path -> !path.endsWith(INDEX)))
                    .sorted((p1, p2) -> {
                        int first = Integer.parseInt(p1.getFileName().toString());
                        int second = Integer.parseInt(p2.getFileName().toString());
                        return Integer.compare(first, second);
                    })
                    .map(path -> {
                        try {
                            Path index = Utils.withSuffix(path, INDEX);
                            return new SSTable(path, index);
                        } catch (IOException e) {
                            Thread.currentThread().interrupt();
                            throw new UncheckedIOException(e);
                        }
                    })
                    .toList();

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
        return null;
    }
}
