package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Entry;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

final class MergedIterator<T extends Comparable<T>, E extends Entry<T>> implements Iterator<E> {
    private final Iterator<E> firstIter;
    private final Iterator<E> secondIter;

    private E firstRecord;
    private E secondRecord;

    private MergedIterator(final Iterator<E> left, final Iterator<E> right) {
        firstIter = right;
        secondIter = left;

        firstRecord = getElement(firstIter);
        secondRecord = getElement(secondIter);
    }

    public static <T extends Comparable<T>, E extends Entry<T>> Iterator<E> instanceOf(List<Iterator<E>> iterators) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }

        var size = iterators.size();
        if (size == 1) {
            return iterators.get(0);
        }

        return new MergedIterator<>(
                instanceOf(iterators.subList(0, size / 2)),
                instanceOf(iterators.subList(size / 2, size))
        );
    }

    @Override
    public boolean hasNext() {
        return firstRecord != null || secondRecord != null;
    }

    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No Such Element");
        }

        final int compareResult = compare(firstRecord, secondRecord);
        final E next = compareResult > 0
                ? secondRecord
                : firstRecord;

        if (compareResult < 0) {
            firstRecord = getElement(firstIter);
        }

        if (compareResult > 0) {
            secondRecord = getElement(secondIter);
        }

        if (compareResult == 0) {
            firstRecord = getElement(firstIter);
            secondRecord = getElement(secondIter);
        }

        return next;
    }

    private int compare(E r1, E r2) {
        if (r1 == null) {
            return 1;
        }

        if (r2 == null) {
            return -1;
        }

        return r1.key().compareTo(r2.key());
    }

    private E getElement(final Iterator<E> iter) {
        return iter.hasNext() ? iter.next() : null;
    }
}
