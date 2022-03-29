package ru.mail.polis.test.arturgaleev;

import java.util.Iterator;

public class PriorityPeekingIterator<E> implements Iterator<E> {
    private final int priority;
    private final Iterator<E> delegate;
    private E current;

    public PriorityPeekingIterator(int priority, Iterator<E> delegate) {
        this.priority = priority;
        this.delegate = delegate;
    }

    public int getPriority() {
        return priority;
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