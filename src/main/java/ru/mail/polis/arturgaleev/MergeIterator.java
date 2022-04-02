package ru.mail.polis.arturgaleev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.PriorityBlockingQueue;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {
    private final PriorityBlockingQueue<PriorityPeekingIterator<Entry<MemorySegment>>> iteratorsQueue;
    private Entry<MemorySegment> currentEntry;

    // Low priority = old value
    // High priority = new value
    public MergeIterator(PriorityPeekingIterator<Entry<MemorySegment>> iterator1,
                         PriorityPeekingIterator<Entry<MemorySegment>> iterator2
    ) {
        iteratorsQueue = new PriorityBlockingQueue<>(2, getComparator());

        if (iterator2.hasNext()) {
            iteratorsQueue.put(iterator2);
        }
        if (iterator1.hasNext()) {
            iteratorsQueue.put(iterator1);
        }
    }

    public MergeIterator(List<PriorityPeekingIterator<Entry<MemorySegment>>> iterators) {
        int iterSize = iterators.isEmpty() ? 1 : iterators.size();
        iteratorsQueue = new PriorityBlockingQueue<>(iterSize, getComparator());

        for (PriorityPeekingIterator<Entry<MemorySegment>> inFilesIterator : iterators) {
            if (inFilesIterator.hasNext()) {
                iteratorsQueue.put(inFilesIterator);
            }
        }
    }

    private static Comparator<PriorityPeekingIterator<Entry<MemorySegment>>> getComparator() {
        return (PriorityPeekingIterator<Entry<MemorySegment>> it1,
                PriorityPeekingIterator<Entry<MemorySegment>> it2
        ) -> {
            if (MemorySegmentComparator.INSTANCE.compare(it1.peek().key(), it2.peek().key()) < 0) {
                return -1;
            } else if (MemorySegmentComparator.INSTANCE.compare(it1.peek().key(), it2.peek().key()) == 0) {
                // reverse compare
                return Long.compare(it2.getPriority(), it1.getPriority());
            } else {
                return 1;
            }
        };
    }

    @Override
    public boolean hasNext() {
        if (currentEntry == null) {
            currentEntry = nullablePeek();
            return currentEntry != null;
        }
        return true;
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> entry = nullableNext();
        if (entry == null) {
            throw new NoSuchElementException();
        } else {
            return entry;
        }
    }

    public Entry<MemorySegment> nullableNext() {
        if (currentEntry != null) {
            Entry<MemorySegment> prev = currentEntry;
            currentEntry = null;
            return prev;
        }
        if (iteratorsQueue.isEmpty()) {
            return null;
        }

        return getNotDeletedElement();
    }

    private Entry<MemorySegment> getNotDeletedElement() {
        PriorityPeekingIterator<Entry<MemorySegment>> iterator = iteratorsQueue.poll();
        Entry<MemorySegment> entry = iterator.next();
        if (iterator.hasNext()) {
            iteratorsQueue.put(iterator);
        }
        removeElementsWithKey(entry.key());

        while (!iteratorsQueue.isEmpty() && entry.value() == null) {
            iterator = iteratorsQueue.poll();
            entry = iterator.next();
            if (iterator.hasNext()) {
                iteratorsQueue.put(iterator);
            }
            removeElementsWithKey(entry.key());
        }

        if (entry.value() == null) {
            return null;
        }
        return entry;
    }

    private void removeElementsWithKey(MemorySegment lastKey) {
        while (!iteratorsQueue.isEmpty() && MemorySegmentComparator.INSTANCE.compare(lastKey, iteratorsQueue.peek().peek().key()) == 0) {
            PriorityPeekingIterator<Entry<MemorySegment>> poll = iteratorsQueue.poll();
            if (poll.hasNext()) {
                poll.next();
                if (poll.hasNext()) {
                    iteratorsQueue.put(poll);
                }
            }
        }
    }

    public Entry<MemorySegment> peek() {
        if (nullablePeek() == null) {
            throw new NoSuchElementException();
        }
        return currentEntry;
    }

    public Entry<MemorySegment> nullablePeek() {
        if (currentEntry == null) {
            currentEntry = nullableNext();
        }
        return currentEntry;
    }
}
