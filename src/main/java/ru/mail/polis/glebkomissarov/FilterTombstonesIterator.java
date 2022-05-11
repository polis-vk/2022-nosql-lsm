package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

import java.util.Iterator;
import java.util.NoSuchElementException;

// Filter tombstones
public class FilterTombstonesIterator implements Iterator<BaseEntry<MemorySegment>> {

    private final MergeIterator iterator;
    private BaseEntry<MemorySegment> current;

    public FilterTombstonesIterator(MergeIterator iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        if (current != null) {
            return true;
        }

        while (iterator.hasNext()) {
            current = iterator.next();
            if (current.value() != null) {
                return true;
            }
        }

        current = null;
        return false;
    }

    @Override
    public BaseEntry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        BaseEntry<MemorySegment> result = current;
        current = null;
        return result;
    }
}
