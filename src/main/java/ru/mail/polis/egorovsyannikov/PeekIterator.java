package ru.mail.polis.egorovsyannikov;

import ru.mail.polis.BaseEntry;

import java.util.Iterator;

public class PeekIterator extends BasePeekIterator {

    private BaseEntry<String> current;
    private final Iterator<BaseEntry<String>> delegate;

    public PeekIterator(Iterator<BaseEntry<String>> delegate) {
        this.delegate = delegate;
        this.generation = 0;
    }

    @Override
    public boolean hasNext() {
        return current != null || delegate.hasNext();
    }

    @Override
    public BaseEntry<String> next() {
        BaseEntry<String> peek = peek();
        current = null;
        return peek;
    }

    @Override
    public BaseEntry<String> peek() {
        if (current == null) {
            current = delegate.next();
        }
        return current;
    }
}
