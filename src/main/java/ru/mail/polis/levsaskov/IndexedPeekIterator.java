package ru.mail.polis.levsaskov;

import ru.mail.polis.Entry;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class IndexedPeekIterator implements Iterator<Entry<ByteBuffer>> {
    private final int storagePartN;
    private final Iterator<Entry<ByteBuffer>> delegate;
    private Entry<ByteBuffer> current;

    public IndexedPeekIterator(Iterator<Entry<ByteBuffer>> delegate, int storagePartN) {
        this.storagePartN = storagePartN;
        this.delegate = delegate;
    }

    public int getStoragePartN() {
        return storagePartN;
    }

    public Entry<ByteBuffer> peek() {
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
    public Entry<ByteBuffer> next() {
        Entry<ByteBuffer> peek = peek();
        current = null;
        return peek;
    }
}
