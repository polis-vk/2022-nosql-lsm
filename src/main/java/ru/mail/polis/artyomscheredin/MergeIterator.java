package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.Collectors;

public class MergeIterator implements Iterator<BaseEntry<ByteBuffer>> {

    private final Queue<PeekIterator> iteratorQueue;

    /**
     * create Merge iterator from PeekIterators.
     *
     * @param iterators - list ordered by ascending iterators priority
     */
    public MergeIterator(List<PeekIterator> iterators) {
        List<PeekIterator> iteratorsCopy = iterators.stream().filter(Objects::nonNull)
                .filter(Iterator::hasNext).collect(Collectors.toList());
        this.iteratorQueue = new PriorityQueue<>();
        iteratorQueue.addAll(iteratorsCopy);
        skipTombStones();
    }

    @Override
    public boolean hasNext() {
        return !iteratorQueue.isEmpty();
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        PeekIterator curr = iteratorQueue.poll();
        if (curr == null) {
            throw new NoSuchElementException();
        }
        BaseEntry<ByteBuffer> result = curr.next();
        if (curr.hasNext()) {
            iteratorQueue.add(curr);
        }
        deleteByKey(result.key());
        skipTombStones();
        return result;
    }

    private void skipTombStones() {
        while (!iteratorQueue.isEmpty() && (iteratorQueue.peek().peek().value() == null)) {
            PeekIterator it = iteratorQueue.poll();
            if (it == null) {
                return;
            }
            ByteBuffer keyToDelete = it.next().key();
            deleteByKey(keyToDelete);
            if (it.hasNext()) {
                iteratorQueue.add(it);
            }
        }
    }

    private void deleteByKey(ByteBuffer keyToDelete) {
        while (!iteratorQueue.isEmpty()) {
            PeekIterator curr = iteratorQueue.poll();
            if (!curr.hasNext()) {
                continue;
            }
            if (!curr.peek().key().equals(keyToDelete)) {
                iteratorQueue.add(curr);
                break;
            }
            curr.next();
            if (curr.hasNext()) {
                iteratorQueue.add(curr);
            }
        }
    }
}
