package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

public class MergeIterator implements Iterator<BaseEntry<String>> {

    private final Deque<PeekIterator> iteratorsQueue = new ArrayDeque<>();
    private BaseEntry<String> next;

    public MergeIterator(Deque<PeekIterator> iterators) {
        this.iteratorsQueue.addAll(iterators);
        this.next = getNextEntry();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public BaseEntry<String> next() {
        BaseEntry<String> result = next;
        next = getNextEntry();
        return result;
    }

    private BaseEntry<String> getNextEntry() {
        BaseEntry<String> result;
        if (!iteratorsQueue.isEmpty()) {
            PeekIterator startIterator = iteratorsQueue.peek();
            PeekIterator currentIterator = iteratorsQueue.poll();
            result = currentIterator.peek();
            iteratorsQueue.add(currentIterator);
            PeekIterator nextIterator = iteratorsQueue.poll();
            while (nextIterator != null && !nextIterator.equals(currentIterator)) {
                if (nextIterator.hasNext()) {
                    BaseEntry<String> newNext = nextIterator.peek();
                    int keyComparison = result == null ? -1 : result.key().compareTo(newNext.key());
                    if (keyComparison > 0) {
                        result = newNext;
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
                if (!nextIterator.equals(startIterator)) {
                    iteratorsQueue.poll();
                    iteratorsQueue.add(nextIterator);
                } else {
                    break;
                }
            }
            if (result == null || result.value() == null) {
                result = getNextEntry();
            }
        } else {
            result = null;
        }
        return result == null || result.value() == null ? null : result;
    }
}
