package ru.mail.polis.pavelkovalenko.iterators;

import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.Serializer;
import ru.mail.polis.pavelkovalenko.comparators.IteratorComparator;
import ru.mail.polis.pavelkovalenko.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentNavigableMap;

public class MergeIterator implements Iterator<Entry<ByteBuffer>> {

    private final Queue<PeekIterator<Entry<ByteBuffer>>> iterators = new PriorityQueue<>(IteratorComparator.INSTANSE);

    public MergeIterator(ByteBuffer from, ByteBuffer to, Serializer serializer,
                         ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> memorySSTable, int sstablesSize)
            throws IOException, ReflectiveOperationException {
        ByteBuffer from1 = from == null ? Utils.EMPTY_BYTEBUFFER : from;
        int priority = 0;

        if (to == null) {
            iterators.add(new PeekIterator<>(memorySSTable.tailMap(from1).values().iterator(), priority++));
        } else {
            iterators.add(new PeekIterator<>(memorySSTable.subMap(from1, to).values().iterator(), priority++));
        }

        for (; priority <= sstablesSize; ++priority) {
            iterators.add(new PeekIterator<>(
                    new FileIterator(serializer.get(sstablesSize - priority + 1), serializer, from1, to),
                    priority
            ));
        }
    }

    @Override
    public boolean hasNext() {
        iterators.removeIf(this::removeIteratorIf);
        skipTombstones();
        return !iterators.isEmpty();
    }

    @Override
    public Entry<ByteBuffer> next() {
        if (iterators.isEmpty()) {
            throw new NoSuchElementException("No more elements in queue");
        }

        Entry<ByteBuffer> result = iterators.peek().peek();
        fallAndRefresh(result);
        return result;
    }

    private boolean removeIteratorIf(PeekIterator<Entry<ByteBuffer>> iterator) {
        return !iterator.hasNext();
    }

    private void fallAndRefresh(Entry<ByteBuffer> entry) {
        List<PeekIterator<Entry<ByteBuffer>>> toBeRefreshed = fallEntry(entry);
        refreshIterators(toBeRefreshed);
    }

    private List<PeekIterator<Entry<ByteBuffer>>> fallEntry(Entry<ByteBuffer> entry) {
        List<PeekIterator<Entry<ByteBuffer>>> toBeRefreshed = new ArrayList<>();
        for (PeekIterator<Entry<ByteBuffer>> iterator : iterators) {
            if (iterator.peek() != null && iterator.peek().key().equals(entry.key())) {
                toBeRefreshed.add(iterator);
                iterator.next();
            }
        }
        return toBeRefreshed;
    }

    private void skipTombstones() {
        while (!iterators.isEmpty() && hasTombstoneForFirstElement()) {
            if (iterators.size() == 1) {
                skipLastOneStanding();
                return;
            }

            PeekIterator<Entry<ByteBuffer>> first = iterators.peek();
            while (first != null && first.hasNext() && Utils.isTombstone(first.peek())) {
                fallAndRefresh(first.peek());
                first = iterators.peek();
            }
        }
    }

    private void skipLastOneStanding() {
        if (iterators.isEmpty()) {
            return;
        }

        PeekIterator<Entry<ByteBuffer>> first = iterators.peek();
        while (first.hasNext() && Utils.isTombstone(first.peek())) {
            first.next();
        }

        if (!first.hasNext()) {
            iterators.remove(first);
        }
    }

    private boolean hasTombstoneForFirstElement() {
        PeekIterator<Entry<ByteBuffer>> first = iterators.remove();
        iterators.add(first);
        return !iterators.isEmpty() && Utils.isTombstone(iterators.peek().peek());
    }

    private void refreshIterators(List<PeekIterator<Entry<ByteBuffer>>> toBeRefreshed) {
        iterators.removeAll(toBeRefreshed);
        for (PeekIterator<Entry<ByteBuffer>> it : toBeRefreshed) {
            if (it.hasNext()) {
                iterators.add(it);
            }
        }
    }

}
