package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Queue;

public class MergeIterator implements Iterator<BaseEntry<String>> {

    private final Deque<PeekIterator> iteratorsQueue = new ArrayDeque<>();
    private BaseEntry<String> next;

    public MergeIterator(Queue<PeekIterator> iterators) {
        this.iteratorsQueue.addAll(iterators);
        next();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public BaseEntry<String> next() {
        BaseEntry<String> result = next;
        if (!iteratorsQueue.isEmpty()) {
            PeekIterator startIterator = iteratorsQueue.peek();
            PeekIterator currentIterator = iteratorsQueue.poll();
            next = currentIterator.peek();
            iteratorsQueue.add(currentIterator);
            PeekIterator nextIterator = iteratorsQueue.poll();
            while (nextIterator != currentIterator) {
                if (nextIterator != null && nextIterator.hasNext()) {
                    BaseEntry<String> newNext = nextIterator.peek();
                    int keyComparison = next == null ? -1 : next.key().compareTo(newNext.key());
                    if (keyComparison > 0) {
                        next = newNext;
                        currentIterator = nextIterator;
                    } else if (keyComparison == 0) {
                        nextIterator.next();
                    }
                    iteratorsQueue.add(nextIterator);
                }
                nextIterator = iteratorsQueue.poll();
            }
            currentIterator.next();
            if (currentIterator.hasNext()) {
                iteratorsQueue.addFirst(currentIterator);
            }
            for (int i = 0; i < iteratorsQueue.size(); i++) {
                nextIterator = iteratorsQueue.peek();
                if (nextIterator != startIterator) {
                    iteratorsQueue.poll();
                    iteratorsQueue.add(nextIterator);
                } else {
                    break;
                }
            }
            if (next == null || next.value() == null) {
                next();
            }
        } else {
            next = null;
        }
        return result == null || result.value() == null ? null : result;
    }
}
