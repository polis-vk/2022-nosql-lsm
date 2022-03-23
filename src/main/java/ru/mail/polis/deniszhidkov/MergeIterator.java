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
        BaseEntry<String> result = null;
        if (!iteratorsQueue.isEmpty()) {
            PeekIterator startIterator = iteratorsQueue.peek();
            PeekIterator currentIterator = iteratorsQueue.poll();
            result = searchForNextEntry(currentIterator);
            backToStart(startIterator);
            if (result == null || result.value() == null) {
                result = getNextEntry();
            }
        }
        return result == null || result.value() == null ? null : result;
    }

    private void backToStart(PeekIterator startIterator) {
        for (int i = 0; i < iteratorsQueue.size(); i++) {
            PeekIterator nextIterator = iteratorsQueue.peek();
            if (nextIterator.equals(startIterator)) {
                break;
            }
            iteratorsQueue.poll();
            iteratorsQueue.add(nextIterator);
        }
    }

    private BaseEntry<String> searchForNextEntry(PeekIterator currentIterator) {
        PeekIterator resultIterator = currentIterator;
        BaseEntry<String> result = currentIterator.peek();
        iteratorsQueue.add(resultIterator);
        PeekIterator nextIterator = iteratorsQueue.poll();
        while (nextIterator != null && !nextIterator.equals(resultIterator)) {
            if (nextIterator.hasNext()) {
                BaseEntry<String> newNext = nextIterator.peek();
                int keyComparison = result == null ? -1 : result.key().compareTo(newNext.key());
                if (keyComparison > 0) {
                    result = newNext;
                    resultIterator = nextIterator;
                } else if (keyComparison == 0) {
                    nextIterator.next();
                }
                iteratorsQueue.add(nextIterator);
            }
            nextIterator = iteratorsQueue.poll();
        }
        resultIterator.next();
        if (resultIterator.hasNext()) {
            iteratorsQueue.addFirst(resultIterator);
        }
        return result;
    }
}
