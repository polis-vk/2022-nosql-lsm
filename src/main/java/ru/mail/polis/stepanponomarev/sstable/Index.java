package ru.mail.polis.stepanponomarev.sstable;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.stepanponomarev.OSXMemorySegment;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

final class Index implements Closeable {
    private static final String FILE_NAME = "ss.index";

    private final MemorySegment tableMemorySegment;
    private final MemorySegment indexMemorySegment;

    private Index(MemorySegment mappedIndex, MemorySegment mappedTable) {
        this.indexMemorySegment = mappedIndex;
        this.tableMemorySegment = mappedTable;
    }

    public static Index upInstance(Path path, MemorySegment tableMemorySegment) throws IOException {
        final Path file = path.resolve(FILE_NAME);
        if (Files.notExists(file)) {
            throw new IllegalArgumentException("File" + path + " is not exits.");
        }

        final MemorySegment indexMemorySegment = MemorySegment.mapFile(
                file,
                0,
                Files.size(file),
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.newSharedScope()
        );

        return new Index(indexMemorySegment, tableMemorySegment);
    }

    public static Index createInstance(
            Path path,
            long[] positions,
            MemorySegment tableMemorySegment
    ) throws IOException {
        final Path file = path.resolve(FILE_NAME);
        Files.createFile(file);
        flush(file, positions);

        final MemorySegment indexMemorySegment = MemorySegment.mapFile(
                file,
                0,
                (long) positions.length * Long.BYTES,
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.newSharedScope()
        );

        return new Index(indexMemorySegment, tableMemorySegment);
    }

    private static void flush(Path file, long... positions) throws IOException {
        final MemorySegment memorySegment = MemorySegment.mapFile(file,
                0,
                (long) positions.length * Long.BYTES,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.newSharedScope()
        );

        long offset = 0;
        for (long pos : positions) {
            MemoryAccess.setLongAtOffset(memorySegment, offset, pos);
            offset += Long.BYTES;
        }

        memorySegment.force();
    }

    @Override
    public void close() throws IOException {
        indexMemorySegment.scope().close();
    }

    public long getKeyPosition(OSXMemorySegment key) {
        if (key == null) {
            return -1;
        }

        long left = 0;
        long right = indexMemorySegment.byteSize() / Long.BYTES;
        while (right >= left) {
            final long mid = left + (right - left) / 2;

            final long keyPosition = MemoryAccess.getLongAtIndex(indexMemorySegment, mid);
            final long keySize = MemoryAccess.getLongAtOffset(tableMemorySegment, keyPosition);

            final MemorySegment foundKey = tableMemorySegment.asSlice(keyPosition + Long.BYTES, keySize);
            final int compareResult = key.compareTo(new OSXMemorySegment(foundKey));

            if (compareResult == 0) {
                return keyPosition;
            }

            if (compareResult < 0) {
                right = mid - 1;
            }

            if (compareResult > 0) {
                left = mid + 1;
            }
        }

        return -1;
    }
}
