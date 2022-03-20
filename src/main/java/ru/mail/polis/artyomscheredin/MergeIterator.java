package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class MergeIterator implements Iterator<BaseEntry<ByteBuffer>> {

    private final List<PeekIterator> iterators;

    /**
     * @param iterators - list ordered by ascending iterators priority
     */
    public MergeIterator(List<PeekIterator> iterators) {
        List<PeekIterator> iteratorsCopy = new LinkedList<>(iterators);
        Collections.reverse(iteratorsCopy);
        this.iterators = iteratorsCopy.stream().filter(Objects::nonNull)
                .filter(Iterator::hasNext).collect(Collectors.toList());
    }

    @Override
    public boolean hasNext() {
        return !iterators.isEmpty();
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        BaseEntry<ByteBuffer> result = null;
        for (PeekIterator it : iterators) {
            BaseEntry<ByteBuffer> curr = it.peek();
            if ((result == null) || (result.key().compareTo(curr.key()) > 0)) {
                result = curr;
            }
        }
        ListIterator<PeekIterator> listIterator = iterators.listIterator();
        while (listIterator.hasNext()) {
            PeekIterator peekIterator = listIterator.next();
            if (result.key().equals(peekIterator.peek().key())) {
                peekIterator.next();
                if (!peekIterator.hasNext()) {
                    listIterator.remove();
                }
            }
        }
        return result;
    }
}
