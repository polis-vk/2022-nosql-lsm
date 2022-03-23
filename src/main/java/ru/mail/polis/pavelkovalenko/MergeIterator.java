package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

public class MergeIterator implements Iterator<Entry<ByteBuffer>> {

    private final ByteBuffer from;
    private final ByteBuffer to;
    private final Map<ByteBuffer, Entry<ByteBuffer>> mergedData = new TreeMap<>();
    private Iterator<Map.Entry<ByteBuffer, Entry<ByteBuffer>>> mergedDataIterator = mergedData.entrySet().iterator();
    private final List<PeekIterator> iterators = new ArrayList<>();
    private final List<Entry<ByteBuffer>> lastEntries = new ArrayList<>();

    public MergeIterator(ByteBuffer from, ByteBuffer to, ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> data,
                NavigableMap<Integer, Entry<Path>> pathsToPairedFiles) throws IOException {
        if (from == null) {
            from = Utils.EMPTY_BYTEBUFFER;
        }

        this.from = from;
        this.to = to;

        if (to == null) {
            iterators.add(new PeekIterator(data.tailMap(from).values().iterator()));
        } else {
            iterators.add(new PeekIterator(data.subMap(from, to).values().iterator()));
        }

        for (Entry<Path> entry: pathsToPairedFiles.values()) {
            iterators.add(new PeekIterator(new FileIterator(entry.key(), entry.value(), from, to)));
        }

        merge();
    }

    @Override
    public boolean hasNext() {
        iterators.removeIf(this::removeIteratorIf);
        return mergedDataIterator.hasNext() || !iterators.isEmpty();
    }

    @Override
    public Entry<ByteBuffer> next() {
        if (!hasNext()) {
            throw new IndexOutOfBoundsException("Out-of-bound merge iteration");
        }
        if (!mergedDataIterator.hasNext()) {
            merge();
        }
        return mergedDataIterator.next().getValue();
    }

    private void merge() {
        mergedData.clear();

        Entry<ByteBuffer> curEntry = Utils.EMPTY_ENTRY;
        while (!isThresholdReached() && hasNext()) {
            peekAll();
            Entry<ByteBuffer> firstMin = findFirstMin();
            fallEntry(firstMin);
            Entry<ByteBuffer> secondMin = findSecondMin(firstMin);
            int minIndex = lastEntries.indexOf(firstMin);

            putIfNotTombstone(firstMin);
            while (Utils.entryComparator.compare(curEntry, secondMin) <= 0
                        && !isThresholdReached()
                        && iterators.get(minIndex).hasNext()) {
                curEntry = iterators.get(minIndex).next();
                putIfNotTombstone(curEntry);
            }

        }

        mergedDataIterator = mergedData.entrySet().iterator();
    }

    private void putIfNotTombstone(Entry<ByteBuffer> entry) {
        if (!Utils.isTombstone(entry)) {
            mergedData.put(entry.key(), entry);
        } else {
            fallEntry(entry);
        }
    }

    private boolean removeIteratorIf(PeekIterator iterator) {
        return !iterator.hasNext();
    }

    private void fallEntry(Entry<ByteBuffer> entry) {
        for (int i = 0; i < lastEntries.size(); ++i) {
            if (lastEntries.get(i).key().equals(entry.key())) {
                iterators.get(i).next();
            }
        }
    }

    private boolean isThresholdReached() {
        return mergedData.size() >= Utils.DATA_PORTION;
    }

    private void peekAll() {
        lastEntries.clear();
        for (PeekIterator iterator: iterators) {
            lastEntries.add(iterator.peek());
        }
    }

    private Entry<ByteBuffer> findFirstMin() {
        return lastEntries.stream()
                .min(Utils.entryComparator)
                .get();
    }

    private Entry<ByteBuffer> findSecondMin(Entry<ByteBuffer> firstMin) {
        return lastEntries.stream()
                .filter(a -> !a.equals(firstMin))
                .min(Utils.entryComparator)
                .orElse(null);
    }

}
