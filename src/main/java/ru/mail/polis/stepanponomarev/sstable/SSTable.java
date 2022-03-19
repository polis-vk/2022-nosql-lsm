package ru.mail.polis.stepanponomarev.sstable;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Entry;
import ru.mail.polis.stepanponomarev.OSXMemorySegment;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;

public final class SSTable implements Closeable {
    public static final int TOMBSTONE_TAG = -1;
    private static final String FILE_NAME = "ss.data";

    private final Index index;
    private final MemorySegment tableMemorySegment;

    private SSTable(Index index, MemorySegment tableMemorySegment) {
        this.index = index;
        this.tableMemorySegment = tableMemorySegment;
    }

    public static SSTable createInstance(
            Path path,
            Iterator<Entry<OSXMemorySegment>> data,
            long dataSize,
            int dataAmount
    ) throws IOException {
        final Path file = path.resolve(FILE_NAME);
        Files.createFile(file);

        final long fileSize = (long) Long.BYTES * 2 * dataAmount + dataSize;
        final long[] positions = flushAndAndGetPositions(file, data, fileSize, dataAmount);
        final MemorySegment tableMemorySegment = MemorySegment.mapFile(
                file,
                0,
                fileSize,
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.newConfinedScope()
        );

        return new SSTable(
                Index.createInstance(path, positions, tableMemorySegment),
                tableMemorySegment
        );
    }

    public static SSTable upInstance(Path path) throws IOException {
        final Path file = path.resolve(FILE_NAME);
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("File" + path + " is not exits.");
        }

        final MemorySegment memorySegment = MemorySegment.mapFile(
                file,
                0,
                Files.size(file),
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.newSharedScope()
        );
        final Index index = Index.upInstance(path, memorySegment);

        return new SSTable(index, memorySegment);
    }

    private static long[] flushAndAndGetPositions(
            Path file,
            Iterator<Entry<OSXMemorySegment>> data,
            long fileSize,
            int dataAmount
    ) throws IOException {
        try (ResourceScope scope = ResourceScope.newSharedScope()) {
            MemorySegment memorySegment = MemorySegment.mapFile(
                    file,
                    0,
                    fileSize,
                    FileChannel.MapMode.READ_WRITE,
                    scope
            );

            int i = 0;
            final long[] positions = new long[dataAmount];

            long currentOffset = 0;
            while (data.hasNext()) {
                positions[i++] = currentOffset;

                final Entry<OSXMemorySegment> entry = data.next();
                final MemorySegment key = entry.key().getMemorySegment();
                final long keySize = key.byteSize();
                MemoryAccess.setLongAtOffset(memorySegment, currentOffset, keySize);
                currentOffset += Long.BYTES;

                memorySegment.asSlice(currentOffset, keySize).copyFrom(key);
                currentOffset += keySize;

                final OSXMemorySegment value = entry.value();
                if (value == null) {
                    MemoryAccess.setLongAtOffset(memorySegment, currentOffset, TOMBSTONE_TAG);
                    currentOffset += Long.BYTES;
                    continue;
                }

                final long valueSize = value.getMemorySegment().byteSize();
                MemoryAccess.setLongAtOffset(memorySegment, currentOffset, valueSize);
                currentOffset += Long.BYTES;

                memorySegment.asSlice(currentOffset, valueSize).copyFrom(value.getMemorySegment());
                currentOffset += valueSize;
            }

            return positions;
        }
    }

    @Override
    public void close() throws IOException {
        index.close();
        tableMemorySegment.scope().close();
    }

    public Iterator<Entry<OSXMemorySegment>> get(OSXMemorySegment from, OSXMemorySegment to) throws IOException {
        final long size = tableMemorySegment.byteSize();
        if (size == 0) {
            return Collections.emptyIterator();
        }

        final long fromPosition = getKeyPositionOrDefault(from, 0);
        final long toPosition = getKeyPositionOrDefault(to, size);

        return new MappedIterator(tableMemorySegment.asSlice(fromPosition, toPosition - fromPosition));
    }

    private long getKeyPositionOrDefault(OSXMemorySegment key, long defaultPosition) {
        final long keyPosition = index.getKeyPosition(key);
        if (keyPosition == -1) {
            return defaultPosition;
        }

        return keyPosition;
    }
}
