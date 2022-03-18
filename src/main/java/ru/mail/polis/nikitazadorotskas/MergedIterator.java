package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

import java.util.*;

public class MergedIterator implements Iterator<BaseEntry<MemorySegment>> {
    private final Utils utils;
    private BaseEntry<MemorySegment> next;
    PriorityQueue<PeekIterator> minHeap = new PriorityQueue<>(new Comparator<>() {
        @Override
        public int compare(PeekIterator first, PeekIterator second) {
            return utils.compareBaseEntries(first.current(), second.current());
        }
    });

    public MergedIterator(List<PeekIterator> iterators, Utils utils) {
        this.utils = utils;
        addIteratorsToHeap(iterators);
        updateNext();
    }

    private void addIteratorsToHeap(List<PeekIterator> iterators) {
        for (PeekIterator iterator : iterators) {
            if (iterator.hasNext()) {
                iterator.next();
                minHeap.add(iterator);
            }
        }
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

    private void updateNext() {
        BaseEntry<MemorySegment> result = null;

        while (result == null && minHeap.size() > 0) {
            PeekIterator iterator = minHeap.poll();
            BaseEntry<MemorySegment> current = iterator.current();

            List<PeekIterator> iterators = getIteratorsWithSameValues(current);
            iterators.add(iterator);

            result = getActualEntry(iterators);
            addIteratorsToHeap(iterators);
        }

        next = result;
    }

    private List<PeekIterator> getIteratorsWithSameValues(BaseEntry<MemorySegment> current) {
        List<PeekIterator> result = new ArrayList<>();

        while (minHeap.size() > 0 && utils.compareBaseEntries(minHeap.peek().current(), current) == 0) {
            result.add(minHeap.poll());
        }

        return result;
    }

    private BaseEntry<MemorySegment> getActualEntry(List<PeekIterator> iterators) {
        int maxNumber = iterators.get(0).getNumber();
        BaseEntry<MemorySegment> result = iterators.get(0).current();
        for (int i = 1; i < iterators.size(); i++) {
            if (iterators.get(i).getNumber() > maxNumber) {
                maxNumber = iterators.get(i).getNumber();
                result = iterators.get(i).current();
            }
        }
        return result.value() == null ? null : result;
    }
}
