package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.Entry;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class PeekIterator implements Iterator<Entry<ByteBuffer>> {

    private final Iterator<Entry<ByteBuffer>> delegate;
    private Entry<ByteBuffer> current;
    private final int priority;

    public PeekIterator(Iterator<Entry<ByteBuffer>> delegate, int priority) {
        this.delegate = delegate;
        this.priority = priority;
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

    public Entry<ByteBuffer> peek() {
        if (current == null && delegate.hasNext()) {
            current = delegate.next();
        }
        return current;
    }

    public int getPriority() {
        return priority;
    }

}
