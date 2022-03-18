package ru.mail.polis.artyomtrofimov;

import ru.mail.polis.Entry;
import java.util.Iterator;

public class PeekingIterator implements Iterator<Entry<String>> {

    private final Iterator<Entry<String>> iterator;
    private Entry<String> currentEntry;

    public PeekingIterator(Iterator<Entry<String>> iterator) {
        this.iterator = iterator;
    }

    public Entry<String> peek() {
        if (currentEntry == null) {
            currentEntry = iterator.next();
        }
        return currentEntry;
    }

    @Override
    public boolean hasNext() {
        return currentEntry != null || iterator.hasNext();
    }

    @Override
    public Entry<String> next() {
        Entry<String> peek = peek();
        currentEntry = null;
        return peek;
    }
}
