package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;

import java.util.Iterator;
import java.util.Queue;

public class MergeIterator implements Iterator<BaseEntry<String>> {

    private final Queue<PriorityPeekIterator> iteratorsQueue;
    private BaseEntry<String> next;

    public MergeIterator(Queue<PriorityPeekIterator> iteratorsQueue) {
        this.iteratorsQueue = iteratorsQueue;
        this.next = getNextEntry();
    }

    @Override
    public boolean hasNext() {
        return (next != null || !iteratorsQueue.isEmpty());
    }

    @Override
    public BaseEntry<String> next() {
        BaseEntry<String> result = next;
        next = getNextEntry();
        return result;
    }

    private BaseEntry<String> getNextEntry() {
        BaseEntry<String> newNext = getNewNext();
        while (!iteratorsQueue.isEmpty()) {
            PriorityPeekIterator nextIterator = iteratorsQueue.poll();
            if (newNext != null && nextIterator.peek().key().compareTo(newNext.key()) == 0) {
                nextIterator.next();
                if (nextIterator.hasNext()) {
                    iteratorsQueue.add(nextIterator);
                }
            } else {
                iteratorsQueue.add(nextIterator);
                if (newNext == null || newNext.value() == null) {
                    newNext = getNewNext();
                    continue;
                }
                break;
            }
        }
        return newNext == null || newNext.value() == null ? null : newNext;
    }

    private BaseEntry<String> getNewNext() {
        if (iteratorsQueue.isEmpty()) {
            return null;
        }
        PriorityPeekIterator currentIterator = iteratorsQueue.poll();
        BaseEntry<String> res = currentIterator.next();
        if (currentIterator.hasNext()) {
            iteratorsQueue.add(currentIterator);
        }
        return res;
    }
}
