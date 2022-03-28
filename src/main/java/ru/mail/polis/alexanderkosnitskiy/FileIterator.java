package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class FileIterator implements Iterator<BaseEntry<ByteBuffer>> {
    private final DaoReader reader;
    private BaseEntry<ByteBuffer> nextValue;
    private final ByteBuffer to;
    private boolean hasNext;

    public FileIterator(DaoReader reader, ByteBuffer initialKey, ByteBuffer to) {
        this.reader = reader;
        if (initialKey == null) {
            nextValue = reader.getFirstEntry();
        } else {
            nextValue = reader.nonPreciseBinarySearch(initialKey);
        }
        this.to = to;
        hasNext = checkNext();
    }

    private boolean checkNext() {
        return nextValue != null && (to == null || to.compareTo(nextValue.key()) > 0);
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        if (!hasNext()) {
            return null;
        }
        BaseEntry<ByteBuffer> temp = nextValue;
        nextValue = reader.getNextEntry();
        hasNext = checkNext();
        return temp;
    }

    public BaseEntry<ByteBuffer> peek() {
        if (!hasNext()) {
            return null;
        }
        return nextValue;
    }
}
