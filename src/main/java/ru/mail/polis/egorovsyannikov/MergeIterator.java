package ru.mail.polis.egorovsyannikov;

import ru.mail.polis.BaseEntry;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

public class MergeIterator implements Iterator<BaseEntry<String>> {

    private Deque<FilePeekIterator> dequeOfIterators;
    private FilePeekIterator currentIterator;

    public MergeIterator(Deque<FilePeekIterator> dequeOfIterators) {
        this.dequeOfIterators = dequeOfIterators;
        currentIterator = dequeOfIterators.poll();
    }

    @Override
    public boolean hasNext() {
        boolean result = false;
        for(FilePeekIterator filePeekIterator: dequeOfIterators) {
            result |= filePeekIterator.hasNext();
        }
        return result || currentIterator.hasNext();
    }

    @Override
    public BaseEntry<String> next() {
        Deque<FilePeekIterator> dequeOfIteratorsSecond = new ArrayDeque<>();
        while (!dequeOfIterators.isEmpty()) {
            FilePeekIterator tempIterator = dequeOfIterators.poll();
            if (currentIterator.peek().key().compareTo(tempIterator.peek().key()) < 0) {
                dequeOfIteratorsSecond.addFirst(currentIterator);
                currentIterator = dequeOfIterators.poll();
            }
            dequeOfIteratorsSecond.addLast(tempIterator);
        }
        dequeOfIterators = dequeOfIteratorsSecond;
        return currentIterator.next();
    }
}
