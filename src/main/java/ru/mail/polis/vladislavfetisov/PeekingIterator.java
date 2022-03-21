package ru.mail.polis.vladislavfetisov;

import java.util.Iterator;
import java.util.NoSuchElementException;

public  class PeekingIterator<T> implements Iterator<T> {
    private final Iterator<T> iterator;
    private T current = null;

    public PeekingIterator(Iterator<T> iterator) {
        this.iterator = iterator;
    }

    public T peek() {
        if (current == null) {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            current = iterator.next();
            return current;
        }
        return current;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext() || current != null;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        T res = peek();
        current = null;
        return res;
    }

}
