package ru.mail.polis.kirillpobedonostsev;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class PeekingIterator implements Iterator<BaseEntry<ByteBuffer>> {

    private BaseEntry<ByteBuffer> lastElement;
    private final Iterator<BaseEntry<ByteBuffer>> iterator;
    private final int priority;

    public PeekingIterator(Iterator<BaseEntry<ByteBuffer>> iterator, int priority) {
        this.iterator = iterator;
        this.priority = priority;
        if (this.iterator.hasNext()) {
            lastElement = iterator.next();
        }
    }

    public int getPriority() {
        return priority;
    }

    public BaseEntry<ByteBuffer> peek() {
        return lastElement;
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        BaseEntry<ByteBuffer> toReturn = lastElement;
        if (iterator.hasNext()) {
            lastElement = iterator.next();
        } else {
            lastElement = null;
        }
        return toReturn;
    }

    @Override
    public boolean hasNext() {
        return lastElement != null;
    }
}
