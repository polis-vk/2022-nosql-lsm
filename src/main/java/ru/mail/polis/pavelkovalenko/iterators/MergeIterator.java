package ru.mail.polis.pavelkovalenko.iterators;

import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.PairedFiles;
import ru.mail.polis.pavelkovalenko.Serializer;
import ru.mail.polis.pavelkovalenko.comparators.IteratorComparator;
import ru.mail.polis.pavelkovalenko.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentNavigableMap;

public class MergeIterator implements Iterator<Entry<ByteBuffer>> {

    private final Queue<PeekIterator<Entry<ByteBuffer>>> iterators = new PriorityQueue<>(IteratorComparator.INSTANSE);

    public MergeIterator(ByteBuffer from, ByteBuffer to, Serializer serializer,
                         ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> memorySSTable,
                         NavigableMap<Integer, PairedFiles> sstables) throws IOException {
        ByteBuffer from1 = from == null ? Utils.EMPTY_BYTEBUFFER : from;
        int priority = 0;

        if (to == null) {
            iterators.add(new PeekIterator<>(memorySSTable.tailMap(from1).values().iterator(), priority++));
        } else {
            iterators.add(new PeekIterator<>(memorySSTable.subMap(from1, to).values().iterator(), priority++));
        }

        for (; priority <= sstables.size(); ++priority) {
            iterators.add(new PeekIterator<>(
                    new FileIterator(serializer.get(priority), serializer, from1, to), priority)
            );
        }
    }

    @Override
    public boolean hasNext() {
        iterators.removeIf(this::removeIteratorIf);
        skipTombstones();
        iterators.removeIf(this::removeIteratorIf);
        return !iterators.isEmpty();
    }

    @Override
    public Entry<ByteBuffer> next() {
        Entry<ByteBuffer> result;
        PeekIterator<Entry<ByteBuffer>> first = iterators.remove();
        PeekIterator<Entry<ByteBuffer>> second = iterators.poll();

        if (second == null) {
            result = first.peek();
            backIterator(first);
        } else {
            int compare = Utils.entryComparator.compare(first.peek(), second.peek());
            if (compare == 0) {
                compare = Integer.compare(first.getPriority(), second.getPriority());
            }

            if (compare < 0) {
                result = first.peek();
            } else if (compare == 0) {
                throw new IllegalStateException("Illegal priority equality");
            } else {
                result = second.peek();
            }

            backIterators(first, second);
        }

        fallEntry(result);
        return result;
    }

    @SafeVarargs
    private void backIterators(PeekIterator<Entry<ByteBuffer>>... peekIterators) {
        Arrays.stream(peekIterators).forEach(this::backIterator);
    }

    private void backIterator(PeekIterator<Entry<ByteBuffer>> it) {
        iterators.add(it);
    }

    private boolean removeIteratorIf(PeekIterator<Entry<ByteBuffer>> iterator) {
        return !iterator.hasNext();
    }

    private void fallEntry(Entry<ByteBuffer> entry) {
        for (PeekIterator<Entry<ByteBuffer>> iterator : iterators) {
            if (iterator.peek() != null && iterator.peek().key().equals(entry.key())) {
                iterator.next();
            }
        }
    }

    private void skipTombstones() {
        while (!iterators.isEmpty() && hasTombstoneForFirstElement()) {
            if (iterators.size() == 1) {
                skipLastOneStanding();
                return;
            }
            skipPairStanding();
        }
    }

    private void skipLastOneStanding() {
        PeekIterator<Entry<ByteBuffer>> first = iterators.remove();
        while (Utils.isTombstone(first.peek()) && first.hasNext()) {
            first.next();
        }
        backIterator(first);
    }

    private void skipPairStanding() {
        PeekIterator<Entry<ByteBuffer>> first = iterators.remove();
        if (iterators.isEmpty()) {
            while (Utils.isTombstone(first.peek()) && first.hasNext()) {
                backIterator(first);
                fallEntry(first.peek());
                refreshIterators();
                first = iterators.remove();
            }
            backIterator(first);
        } else {
            PeekIterator<Entry<ByteBuffer>> second = iterators.poll();

            while (Utils.isTombstone(first.peek()) && first.hasNext()) {
                backIterators(first, second);
                fallEntry(first.peek());
                refreshIterators();
                first = iterators.remove();
                second = iterators.poll();
            }
            backIterators(first, second);
        }
        iterators.removeIf(this::removeIteratorIf);
    }

    private boolean hasTombstoneForFirstElement() {
        refreshIterators();
        return !iterators.isEmpty() && Utils.isTombstone(iterators.peek().peek());
    }

    private void refreshIterators() {
        List<PeekIterator<Entry<ByteBuffer>>> peekIterators = iterators.stream().toList();
        iterators.clear();
        iterators.addAll(peekIterators);
    }

}
