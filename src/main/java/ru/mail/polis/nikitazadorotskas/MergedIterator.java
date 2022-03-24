package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;

public class MergedIterator implements Iterator<BaseEntry<MemorySegment>> {
    private final Utils utils;
    private BaseEntry<MemorySegment> next;
    PriorityQueue<PeekIterator> minHeap = new PriorityQueue<>(this::comparePeekIterators);

    public MergedIterator(List<PeekIterator> iterators, Utils utils) {
        this.utils = utils;
        addIteratorsToHeap(iterators);
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
        if (iterator == null) {
            return;
        }
        if (iterator.hasNext()) {
            iterator.next();
            minHeap.add(iterator);
        }
    }

    private void updateNext() {
        BaseEntry<MemorySegment> result = null;

        while (result == null && minHeap.size() > 0) {
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
        updateNext();
        return result;
    }

    private void addIteratorsWithSameKeyToHeap(BaseEntry<MemorySegment> current) {
        while (minHeap.size() > 0 && utils.compareBaseEntries(minHeap.peek().current(), current) == 0) {
            addIteratorToHeap(minHeap.poll());
        }
    }
}
