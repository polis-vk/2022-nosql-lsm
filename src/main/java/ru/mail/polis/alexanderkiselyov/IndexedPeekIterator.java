package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;

import java.util.Iterator;

public class IndexedPeekIterator implements Iterator<BaseEntry<Byte[]>> {

    private final int index;
    protected final Iterator<BaseEntry<Byte[]>> delegate;
    protected BaseEntry<Byte[]> peek;

    public IndexedPeekIterator(int index, Iterator<BaseEntry<Byte[]>> delegate) {
        this.index = index;
        this.delegate = delegate;
    }

    public int index() {
        return index;
    }

    public BaseEntry<Byte[]> peek() {
        if (peek == null && delegate != null && delegate.hasNext()) {
            peek = delegate.next();
        }
        return peek;
    }

    @Override
    public boolean hasNext() {
        return peek != null || delegate != null && delegate.hasNext();
    }

    @Override
    public BaseEntry<Byte[]> next() {
        BaseEntry<Byte[]> result = peek();
        peek = null;
        return result;
    }
}
