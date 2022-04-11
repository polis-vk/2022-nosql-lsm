package ru.mail.polis.daniilbakin;

import java.util.Iterator;

/** Less order is more priority in merge iterator. */
public class PeekIterator<E> implements Iterator<E> {

    public int order;
    private final Iterator<E> delegate;
    private E current;

    public PeekIterator(Iterator<E> delegate, int order) {
        this.order = order;
        this.delegate = delegate;
    }

    public E peek() {
        if (current == null) {
            current = delegate.next();
        }
        return current;
    }

    @Override
    public boolean hasNext() {
        return current != null || delegate.hasNext();
    }

    @Override
    public E next() {
        E peek = peek();
        current = null;
        return peek;
    }
}
