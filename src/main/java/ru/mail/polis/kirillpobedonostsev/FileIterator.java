package ru.mail.polis.kirillpobedonostsev;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileIterator implements Iterator<BaseEntry<ByteBuffer>> {
    private final ByteBuffer mappedFile;

    public FileIterator(ByteBuffer mappedFile) {
        this.mappedFile = mappedFile;
    }

    @Override
    public boolean hasNext() {
        return mappedFile.remaining() != 0;
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        int keySize = mappedFile.getInt();
        final ByteBuffer key = mappedFile.slice(mappedFile.position(), keySize);
        mappedFile.position(mappedFile.position() + keySize);
        int valueSize = mappedFile.getInt();
        ByteBuffer value = null;
        if (valueSize != PersistenceDao.NULL_VALUE_LENGTH) {
            value = mappedFile.slice(mappedFile.position(), valueSize);
        } else {
            valueSize = 0;
        }
        mappedFile.position(mappedFile.position() + valueSize);
        return new BaseEntry<>(key, value);
    }
}
