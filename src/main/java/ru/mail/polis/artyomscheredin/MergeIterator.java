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
     * create Mergeiterator from PeekIterators.
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
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        PeekIterator curr = iteratorQueue.poll();
        BaseEntry<ByteBuffer> result = curr.next();
        if (curr.hasNext()) {
            iteratorQueue.add(curr);
        }
        deleteByKey(result.key());
        skipTombStones();
        return result;
    }

    private void skipTombStones() {
        if (iteratorQueue.isEmpty()) {
            return;
        }
        while (iteratorQueue.peek().peek().value() == null) {
            PeekIterator it = iteratorQueue.poll();
            ByteBuffer keyToDelete = it.next().key();
            deleteByKey(keyToDelete);
            if (it.hasNext()) {
                iteratorQueue.add(it);
            }

            if (iteratorQueue.isEmpty()) {
                return;
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
