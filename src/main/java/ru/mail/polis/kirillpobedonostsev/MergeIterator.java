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
        removeNull();
        return !queue.isEmpty();
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        PeekingIterator<BaseEntry<ByteBuffer>> nextIter = queue.remove();
        BaseEntry<ByteBuffer> current = nextIter.next();
        skipSame(current);
        if (nextIter.hasNext()) {
            queue.add(nextIter);
        }
        return current;
    }

    private void skipSame(BaseEntry<ByteBuffer> current) {
        if (queue.isEmpty()) {
            return;
        }
        while (!queue.isEmpty()) {
            PeekingIterator<BaseEntry<ByteBuffer>> iter = queue.peek();
            if (!iter.peek().key().equals(current.key())) {
                break;
            }
            iter = queue.remove();
            iter.next();
            if (iter.hasNext()) {
                queue.add(iter);
            }
        }
    }

    private void removeNull() {
        PeekingIterator<BaseEntry<ByteBuffer>> nextIter = queue.remove();
        BaseEntry<ByteBuffer> current = nextIter.peek();
        while (current.value() == null) {
            nextIter.next();
            skipSame(current);
            if (nextIter.hasNext()) {
                queue.add(nextIter);
            }
            if (queue.isEmpty()) {
                break;
            }
            nextIter = queue.remove();
            current = nextIter.peek();
        }
        if (current.value() != null) {
            queue.add(nextIter);
        }
    }
}
