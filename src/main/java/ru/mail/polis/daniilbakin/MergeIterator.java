package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public class MergeIterator<E extends Comparable<E>> implements Iterator<BaseEntry<E>> {

    private BaseEntry<E> next;
    private BaseEntry<E> deleted;
    private final PriorityQueue<PeekIterator<BaseEntry<E>>> minHeap = new PriorityQueue<>(this::compareIterators);

    public MergeIterator(List<PeekIterator<BaseEntry<E>>> iterators) {
        for (PeekIterator<BaseEntry<E>> iterator : iterators) {
            if (iterator.hasNext()) {
                minHeap.add(iterator);
            }
        }
        next = getNext();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public BaseEntry<E> next() {
        BaseEntry<E> res = next;
        next = getNext();
        return res;
    }

    private BaseEntry<E> getNext() {
        PeekIterator<BaseEntry<E>> iterator;
        BaseEntry<E> current;
        while (true) {
            if (minHeap.peek() == null) {
                return null;
            }
            if (!minHeap.peek().hasNext()) {
                minHeap.poll();
                continue;
            }

            iterator = minHeap.poll();
            current = iterator.next();
            minHeap.add(iterator);

            if (checkEntryDeleted(current)) {
                deleted = current;
                continue;
            }
            if (checkNotCorrectEntry(current)) {
                continue;
            }
            return current;
        }
    }

    private boolean checkEntryDeleted(BaseEntry<E> current) {
        return current != null && current.value() == null;
    }

    private boolean checkNotCorrectEntry(BaseEntry<E> current) {
        if (next != null && current != null && current.key().compareTo(next.key()) == 0) {
            return true;
        }
        return deleted != null && current != null && current.key().compareTo(deleted.key()) == 0;
    }

    private int compareIterators(PeekIterator<BaseEntry<E>> first, PeekIterator<BaseEntry<E>> second) {
        if (!first.hasNext()) {
            return 1;
        }
        if (!second.hasNext()) {
            return -1;
        }
        int compare = first.peek().key().compareTo(second.peek().key());
        if (compare == 0) {
            return Integer.compare(first.order, second.order);
        }
        return compare;
    }

}
