package ru.mail.polis.stepanponomarev.SSTable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

class Index implements Closeable {
    private static final String INDEX_FILE_NAME = "ss.index";

    private final Path indexPath;
    private final FileChannel readChannel;

    public Index(Path path) throws IOException {
        indexPath = path.resolve(INDEX_FILE_NAME);

        if (Files.notExists(indexPath)) {
            Files.createFile(indexPath);
        }

        readChannel = FileChannel.open(indexPath, Utils.READ_OPEN_OPTIONS);
    }

    private static ByteBuffer toByteBuffer(Collection<Integer> positions) {
        final ByteBuffer buffer = ByteBuffer.allocate(positions.size() * Integer.BYTES);
        for (int pos : positions) {
            buffer.putInt(pos);
        }
        buffer.flip();

        return buffer;
    }

    public void flushIndex(Collection<Integer> positions) throws IOException {
        final ByteBuffer buffer = toByteBuffer(positions);
        try (FileChannel indexFC = FileChannel.open(indexPath, Utils.APPEND_OPEN_OPTIONS)) {
            indexFC.write(buffer);
        }
    }

    public MappedByteBuffer sliceTable(ByteBuffer from, ByteBuffer to, MappedByteBuffer mappedTable) throws IOException {
        if (from == null && to == null) {
            return mappedTable;
        }

        final int indexChannelSize = (int) readChannel.size();
        final int positionAmount = indexChannelSize / Integer.BYTES;

        final MappedByteBuffer index = readChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannelSize);
        final int[] positions = new int[positionAmount];
        for (int i = 0; i < positionAmount; i++) {
            positions[i] = index.getInt();
        }

        final int size = mappedTable.limit();
        int fromPosition = findKeyPosition(mappedTable, from, positions);
        if (fromPosition == -1) {
            fromPosition = 0;
        }

        int toPosition = findKeyPosition(mappedTable, to, positions);
        if (toPosition == -1) {
            toPosition = size;
        }

        return mappedTable.slice(fromPosition, toPosition - fromPosition);
    }

    private static int findKeyPosition(MappedByteBuffer mappedTable, ByteBuffer key, int[] positions) {
        if (key == null) {
            return -1;
        }

        int left = 0;
        int right = positions.length;
        while (right >= left) {
            final int mid = left + (right - left) / 2;
            final int keySize = mappedTable.position(positions[mid]).getInt();
            final ByteBuffer foundKey = mappedTable.slice(mappedTable.position(), keySize);

            final int compareResult = key.compareTo(foundKey);

            if (compareResult == 0) {
                return positions[mid];
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

    @Override
    public void close() throws IOException {
        readChannel.close();
    }
}
