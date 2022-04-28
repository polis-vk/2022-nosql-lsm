package ru.mail.polis.egorovsyannikov;

import ru.mail.polis.BaseEntry;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

public class MergeIterator implements Iterator<BaseEntry<String>> {

    private Deque<FilePeekIterator> dequeOfIterators;
    private FilePeekIterator currentIterator;
    private BaseEntry<String> current;

    public MergeIterator(Deque<FilePeekIterator> dequeOfIterators) {
        this.dequeOfIterators = dequeOfIterators;
        currentIterator = dequeOfIterators.poll();
        peek();
    }

    public BaseEntry<String> getNext() {
        Deque<FilePeekIterator> dequeOfIteratorsSecond = new ArrayDeque<>();
        while (!dequeOfIterators.isEmpty()) {
            if (!currentIterator.hasNext()) {
                currentIterator = dequeOfIterators.poll();
                if (dequeOfIterators.isEmpty()) {
                    break;
                }
            }
            FilePeekIterator tempIterator = dequeOfIterators.poll();
            if (currentIterator.peek().key().compareTo(tempIterator.peek().key()) > 0) {
                if (currentIterator.hasNext()) {
                    dequeOfIteratorsSecond.addFirst(currentIterator);
                }
                currentIterator = tempIterator;
            } else if (currentIterator.peek().key().compareTo(tempIterator.peek().key()) == 0) {
                if (currentIterator.getGeneration() > tempIterator.getGeneration()) {
                    currentIterator.next();
                    if (currentIterator.hasNext()) {
                        dequeOfIteratorsSecond.addFirst(currentIterator);
                    }
                    currentIterator = tempIterator;
                } else {
                    tempIterator.next();
                    if (tempIterator.hasNext()) {
                        dequeOfIteratorsSecond.addFirst(tempIterator);
                    }
                }
            } else {
                dequeOfIteratorsSecond.addFirst(tempIterator);
            }
        }
        dequeOfIterators = dequeOfIteratorsSecond;
        return currentIterator.hasNext() ? currentIterator.next() : null;
    }

    public void peek() {
        BaseEntry<String> result = getNext();
        while (result != null && result.value() == null) {
            result = getNext();
        }
        current = result;
    }

    @Override
    public boolean hasNext() {
        return current != null;
    }

    @Override
    public BaseEntry<String> next() {
        BaseEntry<String> result = current;
        peek();
        return result;
    }
}
