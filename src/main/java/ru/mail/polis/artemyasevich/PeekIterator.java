package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;

import java.util.Iterator;

public class PeekIterator implements Iterator<BaseEntry<String>>, Comparable<PeekIterator> {
    private final int sourceNumber;
    private final Iterator<BaseEntry<String>> delegate;
    private BaseEntry<String> peeked;

    PeekIterator(Iterator<BaseEntry<String>> iterator, int sourceNumber) {
        this.sourceNumber = sourceNumber;
        this.delegate = iterator;
    }

    @Override
    public boolean hasNext() {
        return peeked != null || delegate.hasNext();
    }

    @Override
    public BaseEntry<String> next() {
        BaseEntry<String> temp = peek();
        peeked = null;
        return temp;
    }

    public BaseEntry<String> peek() {
        if (peeked == null) {
            peeked = delegate.next();
        }
        return peeked;
    }

    @Override
    public int compareTo(PeekIterator o) {
        int keyCompare = this.peek().key().compareTo(o.peek().key());
        if (keyCompare != 0) {
            return keyCompare;
        }
        return this.sourceNumber - o.sourceNumber;
    }
}