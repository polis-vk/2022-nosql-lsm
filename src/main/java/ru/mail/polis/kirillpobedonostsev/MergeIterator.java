package ru.mail.polis.kirillpobedonostsev;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;

class MergeIterator implements Iterator<BaseEntry<ByteBuffer>> {
    private final Queue<PeekingIterator<BaseEntry<ByteBuffer>>> queue;
    private static final Comparator<PeekingIterator<BaseEntry<ByteBuffer>>> comparator =
            Comparator.comparing((PeekingIterator<BaseEntry<ByteBuffer>> iter) -> iter.peek().key())
                    .thenComparing(PeekingIterator::getPriority, Comparator.reverseOrder());
    private static final Comparator<BaseEntry<ByteBuffer>> entryComparator =
            Comparator.comparing(BaseEntry::key);
    private BaseEntry<ByteBuffer> prev;

    public MergeIterator(List<PeekingIterator<BaseEntry<ByteBuffer>>> iterators) {
        queue = new PriorityQueue<>(iterators.size(), comparator);
        for (PeekingIterator<BaseEntry<ByteBuffer>> iterator : iterators) {
            if (iterator.hasNext()) {
                queue.add(iterator);
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (queue.isEmpty()) {
            return false;
        }
        PeekingIterator<BaseEntry<ByteBuffer>> nextIter = queue.remove();
        while (prev != null && nextIter.hasNext() && entryComparator.compare(nextIter.peek(), prev) == 0) {
            nextIter.next();
            if (nextIter.hasNext()) {
                queue.add(nextIter);
            }
            if (queue.isEmpty()) {
                return false;
            } else {
                nextIter = queue.remove();
            }
        }
        if (nextIter.hasNext()) {
            queue.add(nextIter);
        }
        return !queue.isEmpty();
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        PeekingIterator<BaseEntry<ByteBuffer>> nextIter = queue.remove();
        prev = nextIter.next();
        if (nextIter.hasNext()) {
            queue.add(nextIter);
        }
        return prev;
    }
}
