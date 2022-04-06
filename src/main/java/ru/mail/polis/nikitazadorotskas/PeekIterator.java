package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

import java.util.Iterator;

class PeekIterator implements Iterator<BaseEntry<MemorySegment>> {
    private final Iterator<BaseEntry<MemorySegment>> delegate;
    private final int number;
    private BaseEntry<MemorySegment> current;

    PeekIterator(int number, Iterator<BaseEntry<MemorySegment>> delegate) {
        this.delegate = delegate;
        this.number = number;
    }

    public int getNumber() {
        return number;
    }

    public BaseEntry<MemorySegment> current() {
        return current;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public BaseEntry<MemorySegment> next() {
        current = delegate.next();
        return current;
    }
}
