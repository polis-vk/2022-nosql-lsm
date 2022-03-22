package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public class MergeIterator implements Iterator<BaseEntry<String>> {
    private final PriorityQueue<PeekIterator> queue;
    private String keyToSkip;
    private BaseEntry<String> next;

    public MergeIterator(List<PeekIterator> iterators) {
        this.queue = new PriorityQueue<>();
        for (PeekIterator iterator : iterators) {
            if (iterator.hasNext()) {
                queue.add(iterator);
            }
        }
        next = getNext();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public BaseEntry<String> next() {
        BaseEntry<String> nextToGive = next;
        next = getNext();
        return nextToGive;
    }

    private BaseEntry<String> getNext() {
        BaseEntry<String> desiredNext = null;
        while (!queue.isEmpty() && desiredNext == null) {
            PeekIterator current = queue.poll();
            desiredNext = current.next();
            if (desiredNext.value() == null || desiredNext.key().equals(keyToSkip)) {
                if (desiredNext.value() == null) {
                    keyToSkip = desiredNext.key();
                }
                if (current.hasNext()) {
                    queue.add(current);
                }
                desiredNext = null;
                continue;
            }
            keyToSkip = desiredNext.key();
            if (current.hasNext()) {
                queue.add(current);
            }
        }
        return desiredNext;
    }

}
