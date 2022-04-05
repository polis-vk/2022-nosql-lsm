package ru.mail.polis.levsaskov;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class PeekIterator implements Iterator<BaseEntry<ByteBuffer>> {
    private final int storagePartN;
    private final Iterator<BaseEntry<ByteBuffer>> delegate;
    private BaseEntry<ByteBuffer> current;

    public PeekIterator(Iterator<BaseEntry<ByteBuffer>> delegate, int storagePartN) {
        this.storagePartN = storagePartN;
        this.delegate = delegate;
    }

    public int getStoragePartN() {
        return storagePartN;
    }

    public BaseEntry<ByteBuffer> peek() {
        if (current == null && delegate.hasNext()) {
            current = delegate.next();
        }
        return current;
    }

    @Override
    public boolean hasNext() {
        return current != null || delegate.hasNext();
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        BaseEntry<ByteBuffer> peek = peek();
        current = null;
        return peek;
    }
}
