package ru.mail.polis.baidiyarosan;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.PriorityQueue;

public class DaoIterator implements Iterator<BaseEntry<ByteBuffer>> {

    private final PriorityQueue<PeekIterator<BaseEntry<ByteBuffer>>> heap;

    private BaseEntry<ByteBuffer> value;

    public DaoIterator(PriorityQueue<PeekIterator<BaseEntry<ByteBuffer>>> heap) {
        this.heap = heap;
    }

    public BaseEntry<ByteBuffer> peek() {
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
                entry = skipSame(entry, iter.getOrder());
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

    private BaseEntry<ByteBuffer> skipSame(BaseEntry<ByteBuffer> check, int max) {
        PeekIterator<BaseEntry<ByteBuffer>> nextIter;
        BaseEntry<ByteBuffer> entry = check;
        int maxOrder = max;
        while (heap.peek().hasNext() && entry.key().compareTo(heap.peek().peek().key()) == 0) {
            nextIter = heap.poll();
            if (maxOrder < nextIter.getOrder()) {
                entry = nextIter.next();
                maxOrder = nextIter.getOrder();
            } else {
                nextIter.next();
            }
            if (nextIter.hasNext()) {
                heap.add(nextIter);
            }
            if (heap.peek() == null) {
                break;
            }
        }
        return entry;
    }
}