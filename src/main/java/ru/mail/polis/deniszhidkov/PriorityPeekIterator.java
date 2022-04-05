package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;

import java.util.Iterator;

public class PriorityPeekIterator implements Iterator<BaseEntry<String>> {

    private final Iterator<BaseEntry<String>> delegate;
    private final int priorityIndex;
    private BaseEntry<String> current;

    public PriorityPeekIterator(Iterator<BaseEntry<String>> delegate, int priorityIndex) {
        this.delegate = delegate;
        this.priorityIndex = priorityIndex;
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

    public int getPriorityIndex() {
        return priorityIndex;
    }
}
