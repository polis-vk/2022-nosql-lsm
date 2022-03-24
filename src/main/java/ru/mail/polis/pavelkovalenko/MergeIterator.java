package ru.mail.polis.pavelkovalenko;

import java.util.stream.Stream;
import ru.mail.polis.Entry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;

public class MergeIterator implements Iterator<Entry<ByteBuffer>> {

    private final List<PeekIterator> iterators = new ArrayList<>();
    private final List<Entry<ByteBuffer>> lastEntries = new ArrayList<>();

    public MergeIterator(ByteBuffer from, ByteBuffer to, ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> data,
                         NavigableMap<Integer, Entry<Path>> pathsToPairedFiles) throws IOException {
        ByteBuffer from1 = from;
        if (from == null) {
            from1 = Utils.EMPTY_BYTEBUFFER;
        }

        if (to == null) {
            iterators.add(new PeekIterator(data.tailMap(from1).values().iterator()));
        } else {
            iterators.add(new PeekIterator(data.subMap(from1, to).values().iterator()));
        }

        for (Entry<Path> entry: pathsToPairedFiles.values()) {
            iterators.add(new PeekIterator(new FileIterator(entry.key(), entry.value(), from1, to)));
        }
    }

    @Override
    public boolean hasNext() {
        iterators.removeIf(this::removeIteratorIf);
        return !iterators.isEmpty() && iteratorsHaveNext();
    }

    private boolean iteratorsHaveNext() {
        boolean iteratorsHaveNext = false;
        peekAll();
        for (PeekIterator iterator: iterators) {
            iteratorsHaveNext |= iterator.hasNext();
        }
        return iteratorsHaveNext || !lastEntries.isEmpty();
    }

    @Override
    public Entry<ByteBuffer> next() {
        if (lastEntries.isEmpty()) {
            throw new IndexOutOfBoundsException("Out-of-bound merge iteration");
        }
        Entry<ByteBuffer> firstMin = findMin();
        fallEntry(firstMin);
        return firstMin;
    }

    private boolean removeIteratorIf(PeekIterator iterator) {
        return !iterator.hasNext();
    }

    private void fallEntry(Entry<ByteBuffer> entry) {
        for (PeekIterator iterator: iterators) {
            if (iterator.peek() != null && iterator.peek().key().equals(entry.key())) {
                iterator.next();
            }
        }
    }

    private void peekAll() {
        lastEntries.clear();
        for (PeekIterator iterator : iterators) {
            Entry<ByteBuffer> entry = iterator.peek();
            while (iterator.hasNext() && Utils.isTombstone(entry)) {
                fallEntry(entry);
                entry = iterator.peek();
            }
            if (iterator.hasNext()) {
                lastEntries.add(iterator.peek());
            }
        }
    }

    private Entry<ByteBuffer> findMin() {
        return lastEntries.stream()
                .min(Utils.entryComparator)
                .orElse(lastEntries.get(0));
    }

}
