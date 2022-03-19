package ru.mail.polis.daniilbakin;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import ru.mail.polis.BaseEntry;

public class MergeIterator<E extends Comparable<E>> implements Iterator<BaseEntry<E>> {

    private BaseEntry<E> next;
    private BaseEntry<E> deleted;
    private final PriorityQueue<PeekIterator<BaseEntry<E>>> minHeap = new PriorityQueue<>(this::compareIterators);

    public MergeIterator(List<PeekIterator<BaseEntry<E>>> iterators) {
        addIteratorsToHeap(iterators);
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

    private void addIteratorsToHeap(List<PeekIterator<BaseEntry<E>>> iterators) {
        for (PeekIterator<BaseEntry<E>> iterator : iterators) {
            if (iterator.hasNext()) {
                minHeap.add(iterator);
            }
        }
    }

    private BaseEntry<E> getNext() {
        if (minHeap.peek() == null) {
            return null;
        }
        if (minHeap.peek().hasNext()) {
            PeekIterator<BaseEntry<E>> iterator = minHeap.poll();
            BaseEntry<E> next = iterator.next();
            minHeap.add(iterator);
            if (checkEntryDeleted(next)) {
                deleted = next;
                return getNext();
            }
            if (checkNotCorrectNext(next)) {
                return getNext();
            }
            return next;
        }
        minHeap.poll();
        return getNext();
    }

    private boolean checkEntryDeleted(BaseEntry<E> next) {
        return next != null && next.value() == null;
    }

    private boolean checkNotCorrectNext(BaseEntry<E> next) {
        if (this.next != null && next != null && next.key().compareTo(this.next.key()) == 0) {
            return true;
        }
        return deleted != null && next != null && next.key().compareTo(deleted.key()) == 0;
    }

    private int compareIterators(PeekIterator<BaseEntry<E>> first, PeekIterator<BaseEntry<E>> second) {
        if (!first.hasNext()) {
            return 1;
        }
        if (!second.hasNext()) {
            return -1;
        }
        return first.peek().key().compareTo(second.peek().key());
    }

}
