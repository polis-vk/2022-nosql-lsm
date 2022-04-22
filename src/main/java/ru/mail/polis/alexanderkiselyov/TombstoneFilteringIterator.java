package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class TombstoneFilteringIterator implements Iterator<BaseEntry<byte[]>> {
    private final Iterator<BaseEntry<byte[]>> iterator;
    private BaseEntry<byte[]> current;

    public TombstoneFilteringIterator(Iterator<BaseEntry<byte[]>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        if (current != null) {
            return true;
        }

        while (iterator.hasNext()) {
            BaseEntry<byte[]> entry = iterator.next();
            if (!entry.isTombstone()) {
                this.current = entry;
                return true;
            }
        }

        return false;
    }

    @Override
    public BaseEntry<byte[]> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Can't get next element in iterator.");
        }
        BaseEntry<byte[]> next = current;
        current = null;
        return next;
    }
}
