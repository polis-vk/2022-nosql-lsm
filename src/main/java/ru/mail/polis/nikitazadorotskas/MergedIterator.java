package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public class MergedIterator implements Iterator<BaseEntry<MemorySegment>> {
    private final Utils utils;
    private final Iterators iterators;
    private final MemorySegment lastKey;
    private BaseEntry<MemorySegment> next;
    PriorityQueue<PeekIterator> minHeap = new PriorityQueue<>(this::comparePeekIterators);

    public MergedIterator(Iterators iterators, MemorySegment from, MemorySegment to, Utils utils) {
        this.utils = utils;
        this.iterators = iterators;
        this.lastKey = to;
        addIteratorsToHeap(iterators.getPeekIterators(from, to));
        updateNext();
    }

    private int comparePeekIterators(PeekIterator first, PeekIterator second) {
        int compare = utils.compareBaseEntries(first.current(), second.current());
        return compare == 0 ? Integer.compare(second.getNumber(), first.getNumber()) : compare;
    }

    private void addIteratorsToHeap(List<PeekIterator> iterators) {
        for (PeekIterator iterator : iterators) {
            addIteratorToHeap(iterator);
        }
    }

    private void addIteratorToHeap(PeekIterator iterator) {
        if (iterator.hasNext()) {
            iterator.next();
            minHeap.add(iterator);
        }
    }

    private void updateNext() {
        BaseEntry<MemorySegment> result = null;

        while (result == null && !minHeap.isEmpty()) {
            PeekIterator iterator = minHeap.poll();
            result = iterator.current();

            addIteratorsWithSameKeyToHeap(result);
            addIteratorToHeap(iterator);

            result = utils.checkIfWasDeleted(result);
        }

        next = result;
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public BaseEntry<MemorySegment> next() {
        BaseEntry<MemorySegment> result = next;
        try {
            updateNext();
        } catch (IllegalStateException e) {
            minHeap.clear();
            addIteratorsToHeap(iterators.getPeekIterators(next.key(), lastKey));
            updateNext();
        }
        return result;
    }

    private void addIteratorsWithSameKeyToHeap(BaseEntry<MemorySegment> current) {
        while (!minHeap.isEmpty() && utils.compareBaseEntries(minHeap.peek().current(), current) == 0) {
            addIteratorToHeap(minHeap.remove());
        }
    }

    public interface Iterators {
        List<PeekIterator> getPeekIterators(MemorySegment from, MemorySegment to);
    }
}
