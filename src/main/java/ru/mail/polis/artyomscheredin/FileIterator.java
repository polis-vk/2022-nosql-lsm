package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

class FileIterator implements Iterator<BaseEntry<ByteBuffer>> {
    private final ByteBuffer dataBuffer;
    private final ByteBuffer indexBuffer;
    private final int upperBound;
    private int cursor;

    public FileIterator(ByteBuffer dataBuffer, ByteBuffer indexBuffer, ByteBuffer from, ByteBuffer to) {
        this.dataBuffer = dataBuffer;
        this.indexBuffer = indexBuffer;
        cursor = (from == null)
                ? (Storage.getIndexHeaderSize() * Integer.BYTES) : findOffset(indexBuffer, dataBuffer, from);
        upperBound = (to == null) ? indexBuffer.limit() : findOffset(indexBuffer, dataBuffer, to);
    }

    private static int findOffset(ByteBuffer indexBuffer, ByteBuffer dataBuffer, ByteBuffer key) {
        int low = Storage.getIndexHeaderSize();
        int mid;
        int high = indexBuffer.remaining() / Integer.BYTES - 1;
        while (low <= high) {
            mid = low + ((high - low) / 2);
            int offset = indexBuffer.getInt(mid * Integer.BYTES);
            int keySize = dataBuffer.getInt(offset);

            ByteBuffer curKey = dataBuffer.slice(offset + Integer.BYTES, keySize);
            if (curKey.compareTo(key) < 0) {
                low = 1 + mid;
            } else if (curKey.compareTo(key) > 0) {
                high = mid - 1;
            } else if (curKey.compareTo(key) == 0) {
                return mid * Integer.BYTES;
            }
        }
        return low * Integer.BYTES;
    }

    @Override
    public boolean hasNext() {
        return cursor < upperBound;
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        BaseEntry<ByteBuffer> result = Utils.readEntry(dataBuffer, indexBuffer.getInt(cursor));
        cursor += Integer.BYTES;
        return result;
    }
}
