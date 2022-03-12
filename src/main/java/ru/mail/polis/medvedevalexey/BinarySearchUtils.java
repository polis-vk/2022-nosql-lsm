package ru.mail.polis.medvedevalexey;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static ru.mail.polis.medvedevalexey.InMemoryDao.KEY_LENGTH_SIZE;
import static ru.mail.polis.medvedevalexey.InMemoryDao.NUM_OF_ROWS_SIZE;
import static ru.mail.polis.medvedevalexey.InMemoryDao.OFFSET_VALUE_SIZE;
import static ru.mail.polis.medvedevalexey.InMemoryDao.VALUE_LENGTH_SIZE;

class BinarySearchUtils {

    static ByteBuffer binarySearch(FileChannel channel, int rows, ByteBuffer requiredKey) throws IOException {
        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            int mid = (right + left) / 2;
            int cmp = requiredKey.compareTo(keyAt(channel, rows, mid));
            if (cmp < 0) {
                right = mid - 1;
            } else if (cmp > 0) {
                left = mid + 1;
            } else {
                return valueAt(channel, rows, mid);
            }
        }
        return null;
    }

    private static long getOffset(FileChannel channel, int rows, int row) throws IOException {
        return readFromChannel(channel, OFFSET_VALUE_SIZE,
                channel.size() - NUM_OF_ROWS_SIZE - OFFSET_VALUE_SIZE * (rows - row))
                .rewind()
                .getLong();
    }

    private static ByteBuffer keyAt(FileChannel channel, int rows, int row) throws IOException {
        long offset = getOffset(channel, rows, row);
        int keyLength = readFromChannel(channel, KEY_LENGTH_SIZE, offset)
                .rewind()
                .getInt();

        return readFromChannel(channel, keyLength, offset + KEY_LENGTH_SIZE).rewind();
    }

    private static ByteBuffer valueAt(FileChannel channel, int rows, int row) throws IOException {
        long offset = getOffset(channel, rows, row);

        int keyLength = readFromChannel(channel, KEY_LENGTH_SIZE, offset)
                .rewind()
                .getInt();

        int valueLength = readFromChannel(channel, VALUE_LENGTH_SIZE, offset + KEY_LENGTH_SIZE + keyLength)
                .rewind()
                .getInt();

        return readFromChannel(channel, valueLength, offset + KEY_LENGTH_SIZE + keyLength + VALUE_LENGTH_SIZE);
    }

    static ByteBuffer readFromChannel(FileChannel channel, int bufferCapacity, long offset) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(bufferCapacity);
        channel.read(buffer, offset);
        return buffer;
    }

}
