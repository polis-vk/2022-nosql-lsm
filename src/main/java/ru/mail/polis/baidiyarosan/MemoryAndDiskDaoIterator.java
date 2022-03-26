package ru.mail.polis.baidiyarosan;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

public class MemoryAndDiskDaoIterator implements Iterator<BaseEntry<ByteBuffer>> {

    private final PriorityQueue<PeekIterator<BaseEntry<ByteBuffer>>> heap;

    private BaseEntry<ByteBuffer> value;

    public MemoryAndDiskDaoIterator(Collection<PeekIterator<BaseEntry<ByteBuffer>>> collection) {
        this.heap = new PriorityQueue<>(Comparator.comparing((PeekIterator<BaseEntry<ByteBuffer>> i) ->
                i.peek().key()).thenComparingInt(PeekIterator::getOrder));
        this.heap.addAll(collection);
    }

    @Override
    public boolean hasNext() {
        return value != null || peek() != null;
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        BaseEntry<ByteBuffer> peek = peek();
        value = null;
        return peek;
    }

    private BaseEntry<ByteBuffer> peek() {
        if (value == null) {
            PeekIterator<BaseEntry<ByteBuffer>> iter = heap.poll();
            if (iter == null) {
                return null;
            }
            BaseEntry<ByteBuffer> entry = iter.next();
            if (iter.hasNext()) {
                heap.add(iter);
            }
            if (heap.peek() != null) {
                filter(entry);
            }

            if (entry.value() != null) {
                value = entry;
                return value;
            }

            if (heap.peek() != null && heap.peek().hasNext()) {
                return peek();
            }
        }
        return value;
    }

    private void filter(BaseEntry<ByteBuffer> check) {
        PeekIterator<BaseEntry<ByteBuffer>> nextIter;

        while (heap.peek().hasNext() && check.key().compareTo(heap.peek().peek().key()) == 0) {
            nextIter = heap.poll();
            nextIter.next();
            if (nextIter.hasNext()) {
                heap.add(nextIter);
            }
            if (heap.peek() == null) {
                break;
            }
        }
    }
}
