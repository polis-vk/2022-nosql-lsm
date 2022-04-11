package ru.mail.polis.test.arturgaleev;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.PriorityBlockingQueue;

public class MergeIterator implements Iterator<BaseEntry<ByteBuffer>> {
    private final PriorityBlockingQueue<PriorityPeekingIterator<BaseEntry<ByteBuffer>>> iteratorsQueue;
    private BaseEntry<ByteBuffer> currentEntry;

    // Low priority = old value
    // High priority = new value
    public MergeIterator(PriorityPeekingIterator<BaseEntry<ByteBuffer>> iterator1,
                         PriorityPeekingIterator<BaseEntry<ByteBuffer>> iterator2
    ) {
        iteratorsQueue = new PriorityBlockingQueue<>(2, getComparator());

        if (iterator2.hasNext()) {
            iteratorsQueue.put(iterator2);
        }
        if (iterator1.hasNext()) {
            iteratorsQueue.put(iterator1);
        }
    }

    public MergeIterator(List<PriorityPeekingIterator<BaseEntry<ByteBuffer>>> iterators) {
        int iterSize = iterators.isEmpty() ? 1 : iterators.size();
        iteratorsQueue = new PriorityBlockingQueue<>(iterSize, getComparator());

        for (PriorityPeekingIterator<BaseEntry<ByteBuffer>> inFilesIterator : iterators) {
            if (inFilesIterator.hasNext()) {
                iteratorsQueue.put(inFilesIterator);
            }
        }
    }

    private static Comparator<PriorityPeekingIterator<BaseEntry<ByteBuffer>>> getComparator() {
        return (PriorityPeekingIterator<BaseEntry<ByteBuffer>> it1,
                PriorityPeekingIterator<BaseEntry<ByteBuffer>> it2
        ) -> {
            int cmpResult = it1.peek().key().compareTo(it2.peek().key());
            if (cmpResult < 0) {
                return -1;
            } else if (cmpResult == 0) {
                return Integer.compare(it2.getPriority(), it1.getPriority());
            } else {
                return 1;
            }
        };
    }

    @Override
    public boolean hasNext() {
        if (currentEntry == null) {
            currentEntry = nullablePeek();
            return currentEntry != null;
        }
        return true;
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        BaseEntry<ByteBuffer> entry = nullableNext();
        if (entry == null) {
            throw new NoSuchElementException();
        } else {
            return entry;
        }
    }

    public BaseEntry<ByteBuffer> nullableNext() {
        if (currentEntry != null) {
            BaseEntry<ByteBuffer> prev = currentEntry;
            currentEntry = null;
            return prev;
        }
        if (iteratorsQueue.isEmpty()) {
            return null;
        }

        return getNotDeletedElement();
    }

    public String toString(ByteBuffer in) {
        ByteBuffer data = in.asReadOnlyBuffer();
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private BaseEntry<ByteBuffer> getNotDeletedElement() {
        PriorityPeekingIterator<BaseEntry<ByteBuffer>> iterator = iteratorsQueue.poll();
        BaseEntry<ByteBuffer> entry = iterator.next();
        if (iterator.hasNext()) {
            iteratorsQueue.put(iterator);
        }
        removeElementsWithKey(entry.key());

        while (!iteratorsQueue.isEmpty() && entry.value() == null) {
            iterator = iteratorsQueue.poll();
            entry = iterator.next();
            if (iterator.hasNext()) {
                iteratorsQueue.put(iterator);
            }
            removeElementsWithKey(entry.key());
        }

        if (entry.value() == null) {
            return null;
        }
        return entry;
    }

    private void removeElementsWithKey(ByteBuffer lastKey) {
        while (!iteratorsQueue.isEmpty() && lastKey.equals(iteratorsQueue.peek().peek().key())) {
            PriorityPeekingIterator<BaseEntry<ByteBuffer>> poll = iteratorsQueue.poll();
            if (poll.hasNext()) {
                poll.next();
                if (poll.hasNext()) {
                    iteratorsQueue.put(poll);
                }
            }
        }
    }

    public BaseEntry<ByteBuffer> peek() {
        if (nullablePeek() == null) {
            throw new NoSuchElementException();
        }
        return currentEntry;
    }

    public BaseEntry<ByteBuffer> nullablePeek() {
        if (currentEntry == null) {
            currentEntry = nullableNext();
        }
        return currentEntry;
    }
}
