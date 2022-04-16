package ru.mail.polis.pavelkovalenko.iterators;

import java.util.Iterator;

public class PeekIterator<E> implements Iterator<E> {

    private final int priority;
    private final Iterator<E> delegate;
    private E peek;

    public PeekIterator(Iterator<E> delegate, int priority) {
        this.delegate = delegate;
        this.priority = priority;
    }

    @Override
    public boolean hasNext() {
        return peek != null || delegate.hasNext();
    }

    @Override
    public E next() {
        E peek1 = peek();
        this.peek = null;
        return peek1;
    }

    public E peek() {
        if (peek == null && delegate.hasNext()) {
            peek = delegate.next();
        }
        return peek;
    }

    public int getPriority() {
        return priority;
    }

}
