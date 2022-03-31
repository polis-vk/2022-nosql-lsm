package ru.mail.polis.andreyilchenko;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;

public class MergedIterator implements Iterator<BaseEntry<ByteBuffer>> {
    private final Queue<PeekingPriorityIterator> queue = new PriorityQueue<>(priorityComparator());

    public MergedIterator(List<PeekingPriorityIterator> iteratorList) {
        queue.addAll(iteratorList);
    }

    @Override
    public boolean hasNext() {
        clearQueue(queue.iterator());
        while (!queue.isEmpty() && queueElemPeek().value() == null) {
            PeekingPriorityIterator poll = queue.poll();
            updatePeekIterator(Objects.requireNonNull(poll));
        }
        return !queue.isEmpty();
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        BaseEntry<ByteBuffer> nextElem = queue.isEmpty()
                ? queue.peek().next() : updatePeekIterator(queue.poll());
        return nextElem.value() == null ? null : nextElem;
    }

    private BaseEntry<ByteBuffer> updatePeekIterator(PeekingPriorityIterator nextIter) {
        BaseEntry<ByteBuffer> nextEntry = nextIter.next();
        removeEquals(nextEntry);
        addInQueueIfHasNext(nextIter);
        return nextEntry;
    }

    private void removeEquals(BaseEntry<ByteBuffer> nextEntry) {
        while (!queue.isEmpty() && nextEntry.key().equals(queueElemPeek().key())) {
            PeekingPriorityIterator iterator = queue.poll();
            iterator.next();
            addInQueueIfHasNext(iterator);
        }
    }

    private void addInQueueIfHasNext(PeekingPriorityIterator peek) {
        if (peek.hasNext()) {
            queue.add(peek);
        }
    }

    private void clearQueue(Iterator<PeekingPriorityIterator> queueIterator) {
        while (queueIterator.hasNext()) {
            if (!queueIterator.next().hasNext()) {
                queueIterator.remove();
            }
        }
    }

    private BaseEntry<ByteBuffer> queueElemPeek() {
        return queue.peek().peek();
    }

    private Comparator<PeekingPriorityIterator> priorityComparator() {
        return (x, y) -> {
            if (!x.hasNext() || !y.hasNext()) {
                return y.hasNext() ? 1 : -1;
            }
            int compareKeyResult = x.peek().key().compareTo(y.peek().key());
            return compareKeyResult == 0
                    ? Integer.compare(x.getPriority(), y.getPriority()) : compareKeyResult;
        };
    }
}


