package ru.mail.polis.baidiyarosan;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class MergingIterator implements Iterator<BaseEntry<ByteBuffer>> {

    private static final Comparator<PeekIterator<BaseEntry<ByteBuffer>>> COMPARATOR = Comparator.comparing(
            (PeekIterator<BaseEntry<ByteBuffer>> i) ->
                    i.peek().key()).thenComparingInt(PeekIterator::getOrder);

    private final PriorityQueue<PeekIterator<BaseEntry<ByteBuffer>>> heap;

    private BaseEntry<ByteBuffer> value;

    public MergingIterator(Collection<PeekIterator<BaseEntry<ByteBuffer>>> collection) {
        this.heap = new PriorityQueue<>(COMPARATOR);
        this.heap.addAll(collection);
    }

    @Override
    public boolean hasNext() {
        return value != null || peek() != null;
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        BaseEntry<ByteBuffer> peek = peek();
        value = null;
        return peek;
    }

    private BaseEntry<ByteBuffer> peek() {
        if (value == null) {
            do {
                PeekIterator<BaseEntry<ByteBuffer>> iter = heap.poll();
                if (iter == null) {
                    return null;
                }
                BaseEntry<ByteBuffer> entry = iter.next();
                if (iter.hasNext()) {
                    heap.add(iter);
                }

                filter(entry);

                if (entry.value() != null) {
                    value = entry;
                    return value;
                }

            } while (heap.peek() != null && heap.peek().hasNext());
        }
        return value;
    }

    private void filter(BaseEntry<ByteBuffer> check) {
        while (heap.peek() != null && check.key().compareTo(heap.peek().peek().key()) == 0) {
            PeekIterator<BaseEntry<ByteBuffer>> nextIter = heap.poll();
            nextIter.next();
            if (nextIter.hasNext()) {
                heap.add(nextIter);
            }
        }
    }
}
