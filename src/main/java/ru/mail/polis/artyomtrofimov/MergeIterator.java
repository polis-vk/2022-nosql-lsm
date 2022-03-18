package ru.mail.polis.artyomtrofimov;

import ru.mail.polis.Entry;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;

public class MergeIterator implements Iterator<Entry<String>> {
    private final Queue<PeekingIterator> queue = new PriorityQueue<>((l, r) -> {
        if (l.hasNext() && r.hasNext()) {
            return l.peek().key().compareTo(r.peek().key());
        }
        return l.hasNext() ? -1 : 1;
    });

    public MergeIterator(List<PeekingIterator> iterators) {
        queue.addAll(iterators);
    }

    @Override
    public boolean hasNext() {
        queue.removeIf(item -> !item.hasNext());
        return !queue.isEmpty();
    }

    @Override
    public Entry<String> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        PeekingIterator nextIter = queue.poll();
        Entry<String> nextEntry = nextIter.next();
        queue.add(nextIter);
        while (queue.peek().hasNext() && queue.peek().peek().key().equals(nextEntry.key())) {
            PeekingIterator peek = queue.poll();
            peek.next();
            queue.add(peek);
        }
        return nextEntry;
    }
}
