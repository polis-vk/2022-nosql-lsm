package ru.mail.polis.baidiyarosan;

import java.util.Iterator;

public class PeekIterator<E> implements Iterator<E> {

    private final Iterator<E> iter;

    private final int order;

    private E value;

    public PeekIterator(Iterator<E> iter, int order) {
        this.iter = iter;
        this.order = order;
    }

    public int getOrder() {
        return order;
    }

    public E peek() {
        if (value == null) {
            value = iter.next();
        }
        return value;
    }

    @Override
    public boolean hasNext() {
        return value != null || iter.hasNext();
    }

    @Override
    public E next() {
        E peek = peek();
        value = null;
        return peek;
    }

}