package ru.mail.polis.kirillpobedonostsev;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileIterator implements Iterator<BaseEntry<ByteBuffer>> {
    private final ByteBuffer mappedFile;
    private int pos;
    private final int toPos;

    public FileIterator(ByteBuffer mappedFile, int fromPos, int toPos) {
        pos = fromPos;
        this.toPos = toPos;
        this.mappedFile = mappedFile;
    }

    @Override
    public boolean hasNext() {
        return pos < toPos;
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        int offset = mappedFile.getInt(PersistenceDao.FILE_HEADER_SIZE + pos * Integer.BYTES);
        int keySize = mappedFile.getInt(offset);
        offset += Integer.BYTES;
        final ByteBuffer key = mappedFile.slice(offset, keySize);
        offset += keySize;
        int valueSize = mappedFile.getInt(offset);
        offset += Integer.BYTES;
        ByteBuffer value = null;
        if (valueSize == PersistenceDao.NULL_VALUE_LENGTH) {
            valueSize = 0;
        } else {
            value = mappedFile.slice(offset, valueSize);
        }
        pos++;
        return new BaseEntry<>(key, value);
    }
}
