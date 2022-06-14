package ru.mail.polis.vladislavfetisov.wal;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Entry;
import ru.mail.polis.vladislavfetisov.MemorySegments;
import ru.mail.polis.vladislavfetisov.Utils;
import ru.mail.polis.vladislavfetisov.lsm.SSTable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * Not thread safe
 */
class ReadOnlyLog {
    private final MemorySegment recordsFile;
    private final MemorySegment positionsFile;

    private final Path tableName;
    private final Path positionsName;

    private long positionsOffset;

    public Path getTableName() {
        return tableName;
    }

    public Path getPositionsName() {
        return positionsName;
    }

    private ReadOnlyLog(Path tableName, ResourceScope scope) throws IOException {
        this.tableName = tableName;
        this.positionsName = Path.of(tableName + SSTable.INDEX);
        this.recordsFile
                = MemorySegments.map(tableName, Files.size(tableName), FileChannel.MapMode.READ_ONLY, scope);
        long positionsSize = MemoryAccess.getLongAtOffset(recordsFile, 0);
        this.positionsFile =
                MemorySegments.map(positionsName, positionsSize, FileChannel.MapMode.READ_ONLY, scope);
    }

    public boolean isEmpty() {
        return positionsOffset == positionsFile.byteSize();
    }

    public Entry<MemorySegment> peek() {
        if (isEmpty()) {
            return null;
        }
        while (true) {
            long positionChecksum = MemoryAccess.getLongAtOffset(positionsFile, positionsOffset);
            long position = MemoryAccess.getLongAtOffset(positionsFile, positionsOffset + Long.BYTES);
            positionsOffset += WAL.POSITION_LENGTH;
            if (positionChecksum != Utils.getLongChecksum(position)) {
                WAL.LOGGER.info("Checksum is not valid");
                if (isEmpty()) {
                    return null;
                }
                continue;
            }
            return MemorySegments.readEntryByOffset(recordsFile, position);
        }
    }

    public static List<ReadOnlyLog> getReadOnlyLogs(Path dir, ResourceScope scope) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files
                    .filter(path -> {
                        String s = path.toString();
                        return !s.endsWith(SSTable.INDEX);
                    })
                    .mapToInt(Utils::getTableNum)
                    .sorted()
                    .mapToObj(i -> {
                        try {
                            return new ReadOnlyLog(dir.resolve(String.valueOf(i)), scope);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .toList();
        }
    }
}
