package ru.mail.polis.nikitadergunov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

import java.util.Iterator;

public class PeekIterator implements Iterator<Entry<MemorySegment>> {
    private Entry<MemorySegment> currentValue;
    private final Iterator<Entry<MemorySegment>> iterator;
    private boolean hasLast = true;
    public PeekIterator(Iterator<Entry<MemorySegment>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        if (iterator.hasNext()) {
            return true;
        }
        if (hasLast) {
            hasLast = false;
            return true;
        }
        return currentValue != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (iterator.hasNext()) {
            currentValue = iterator.next();
            return currentValue;
        }
        if (currentValue != null) {
            Entry<MemorySegment> copyCurrentValue = currentValue;
            currentValue = null;
            return copyCurrentValue;
        }
        return null;
    }

    public Entry<MemorySegment> peek() {
        return currentValue;
    }
}
