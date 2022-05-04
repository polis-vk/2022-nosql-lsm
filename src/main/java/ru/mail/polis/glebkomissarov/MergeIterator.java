package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

// Filter old
public class MergeIterator implements Iterator<BaseEntry<MemorySegment>> {
    private final PriorityQueue<PeekIterator> iterators = new PriorityQueue<>(
            Comparator::iteratorsCompare
    );

    public MergeIterator(List<PeekIterator> iterators) {
        iterators.removeIf(i -> !i.hasNext());
        this.iterators.addAll(iterators);
    }

    @Override
    public boolean hasNext() {
        return !iterators.isEmpty();
    }

    @Override
    public BaseEntry<MemorySegment> next() {
        PeekIterator next = iterators.remove();

        while (!iterators.isEmpty()) {
            PeekIterator current = iterators.peek();
            if (Comparator.compare(current.peek().key(), next.peek().key()) != 0) {
                break;
            }

            iterators.remove();
            current.next();
            if (current.hasNext()) {
                iterators.add(current);
            }
        }

        BaseEntry<MemorySegment> result = next.next();
        if (next.hasNext()) {
            iterators.add(next);
        }
        return result;
    }
}
