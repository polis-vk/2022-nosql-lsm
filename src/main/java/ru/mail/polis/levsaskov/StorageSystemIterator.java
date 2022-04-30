package ru.mail.polis.levsaskov;

import ru.mail.polis.Entry;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class StorageSystemIterator implements Iterator<Entry<ByteBuffer>> {
    private final PriorityQueue<IndexedPeekIterator> binaryHeap;
    private Entry<ByteBuffer> next;

    public StorageSystemIterator(PriorityQueue<IndexedPeekIterator> binaryHeap) {
        this.binaryHeap = binaryHeap;
    }

    @Override
    public boolean hasNext() {
        if (next == null) {
            next = tryToGetNext();
        }

        return next != null;
    }

    @Override
    public Entry<ByteBuffer> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Entry<ByteBuffer> ans = next;
        next = null;

        return ans;
    }

    private Entry<ByteBuffer> tryToGetNext() {
        while (!binaryHeap.isEmpty()) {
            IndexedPeekIterator freshIterator = binaryHeap.remove();
            Entry<ByteBuffer> freshNext = freshIterator.next();

            while (!binaryHeap.isEmpty() && freshNext.key().equals(binaryHeap.element().peek().key())) {
                IndexedPeekIterator dublicateIt = binaryHeap.remove();
                Entry<ByteBuffer> dublicateNext = dublicateIt.next();
                if (dublicateIt.getStoragePartN() > freshIterator.getStoragePartN()) {
                    IndexedPeekIterator temp = freshIterator;
                    freshIterator = dublicateIt;
                    dublicateIt = temp;

                    freshNext = dublicateNext;
                }

                if (dublicateIt.peek() != null) {
                    binaryHeap.add(dublicateIt);
                }
            }

            if (freshIterator.peek() != null) {
                binaryHeap.add(freshIterator);
            }

            if (freshNext.value() != null) {
                return freshNext;
            }
        }

        return null;
    }
}
