package ru.mail.polis.kirillpobedonostsev;

import java.util.Iterator;

public class PeekingIterator<E> implements Iterator<E> {

    private E lastElement;
    private final Iterator<E> iterator;
    private final int priority;

    public PeekingIterator(Iterator<E> iterator, int priority) {
        this.iterator = iterator;
        this.priority = priority;
        if (this.iterator.hasNext()) {
            lastElement = iterator.next();
        }
    }

    public int getPriority() {
        return priority;
    }

    public E peek() {
        return lastElement;
    }

    @Override
    public E next() {
        E toReturn = lastElement;
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
