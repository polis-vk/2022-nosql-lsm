package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class PeekIterator implements Iterator<BaseEntry<ByteBuffer>>, Comparable<PeekIterator> {
    private final Iterator<BaseEntry<ByteBuffer>> iterator;
    private BaseEntry<ByteBuffer> next = null;
    private final Integer priority;

    public PeekIterator(Iterator<BaseEntry<ByteBuffer>> iterator, int priority) {
        this.iterator = iterator;
        this.priority = priority;
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

    @Override
    public int compareTo(PeekIterator e2) {
        int keyComparison = this.peek().key().compareTo(e2.peek().key());
        if (keyComparison != 0) {
            return keyComparison;
        } else {
            return Integer.compare(e2.priority, this.priority);
        }
    }
}

