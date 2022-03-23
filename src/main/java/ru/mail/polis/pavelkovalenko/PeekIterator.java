package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.Entry;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class PeekIterator implements Iterator<Entry<ByteBuffer>> {

    private final Iterator<Entry<ByteBuffer>> delegate;
    private Entry<ByteBuffer> current = null;

    public PeekIterator(Iterator<Entry<ByteBuffer>> delegate) {
        this.delegate = delegate;
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
