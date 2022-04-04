package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class RangeIterator implements Iterator<BaseEntry<MemorySegment>> {

    private final PriorityQueue<PeekIterator> iterators = new PriorityQueue<>(
            Comparator::iteratorsCompare
    );
    private MemorySegment previous;

    public RangeIterator(List<PeekIterator> iterators) {
        iterators.removeIf(i -> !i.hasNext());
        this.iterators.addAll(iterators);
    }

    @Override
    public boolean hasNext() {
        PeekIterator nextElem;
        while ((nextElem = iterators.peek()) != null) {
            BaseEntry<MemorySegment> current = nextElem.peek();
            if (current.value() == null) {
                previous = current.key();
                reInsert();
                continue;
            }

            if (!isEquals(current.key(), previous)) {
                return true;
            }
            reInsert();
        }
        return false;
    }

    @Override
    public BaseEntry<MemorySegment> next() {
        BaseEntry<MemorySegment> result = reInsert();
        previous = result.key();
        return result;
    }

    private BaseEntry<MemorySegment> reInsert() {
        PeekIterator nextElem = iterators.poll();
        if (nextElem == null) {
            throw new NoSuchElementException();
        }

        BaseEntry<MemorySegment> res = nextElem.next();
        if (nextElem.hasNext()) {
            iterators.add(nextElem);
        }
        return res;
    }

    private boolean isEquals(MemorySegment o1, MemorySegment o2) {
        if (o2 == null) {
            return false;
        }
        return Comparator.compare(o1, o2) == 0;
    }
}
