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
    private final Queue<PeekingIterator> queue;
    private static final Comparator<PeekingIterator> comparator = Comparator.comparing(i -> i.peek().key());
    private static final Comparator<BaseEntry<ByteBuffer>> entryComparator = Comparator.comparing(BaseEntry::key);
    private BaseEntry<ByteBuffer> prev;

    public MergeIterator(List<Iterator<BaseEntry<ByteBuffer>>> iterators) {
        queue = new PriorityQueue<>(iterators.size(),
                comparator.thenComparing(PeekingIterator::getPriority));
        for (int i = 0; i < iterators.size(); i++) {
            Iterator<BaseEntry<ByteBuffer>> iterator = iterators.get(i);
            if (iterator.hasNext()) {
                queue.add(new PeekingIterator(iterator, i));
            }
        }
    }

    public boolean hasNext() {
        if (queue.isEmpty()) {
            return false;
        }
        PeekingIterator nextIter = queue.remove();
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

    public BaseEntry<ByteBuffer> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        PeekingIterator nextIter = queue.remove();
        prev = nextIter.next();
        if (nextIter.hasNext()) {
            queue.add(nextIter);
        }
        return prev;
    }
}
