package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;

import java.util.Iterator;

public class PeekIterator implements Iterator<BaseEntry<String>> {

    private final Iterator<BaseEntry<String>> delegate;
    private BaseEntry<String> current;

    public PeekIterator(Iterator<BaseEntry<String>> delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        return current != null || delegate.hasNext();
    }

    public BaseEntry<String> peek() {
        if (current == null) {
            current = delegate.next();
        }
        return current;
    }

    @Override
    public BaseEntry<String> next() {
        BaseEntry<String> peek = peek();
        current = null;
        return peek;
    }
}
