package ru.mail.polis.andreyilchenko;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

class PeekingPriorityIterator implements Iterator<BaseEntry<ByteBuffer>> {
    BaseEntry<ByteBuffer> nextElem;
    Iterator<BaseEntry<ByteBuffer>> defaultIterator;
    private final int priority;

    public PeekingPriorityIterator(Iterator<BaseEntry<ByteBuffer>> iterator, int priority) {
        if (iterator.hasNext()) {
            this.nextElem = iterator.next();
        }
        this.defaultIterator = iterator;
        this.priority = priority;
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        if (nextElem == null) {
            throw new NoSuchElementException();
        }
        BaseEntry<ByteBuffer> returnElem = nextElem;
        nextElem = defaultIterator.hasNext() ? defaultIterator.next() : null;
        return returnElem;
    }

    @Override
    public boolean hasNext() {
        return nextElem != null;
    }

    public int getPriority() {
        return priority;
    }

    public BaseEntry<ByteBuffer> peek() {
        if (nextElem == null) {
            throw new NoSuchElementException();
        }
        return nextElem;
    }
}
