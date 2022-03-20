package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class PeekIterator implements Iterator<BaseEntry<ByteBuffer>> {
    private final Iterator<BaseEntry<ByteBuffer>> iterator;
    private BaseEntry<ByteBuffer> next = null;

    public PeekIterator(Iterator<BaseEntry<ByteBuffer>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return (next != null) || iterator.hasNext();
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        BaseEntry<ByteBuffer> curr = peek();
        next = null;
        return curr;
    }

    public BaseEntry<ByteBuffer> peek() {
        if (next == null) {
            next = iterator.next();
        }
        return next;
    }
}
