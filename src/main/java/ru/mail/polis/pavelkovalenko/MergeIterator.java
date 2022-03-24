package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.Entry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MergeIterator implements Iterator<Entry<ByteBuffer>> {

    private final Map<ByteBuffer, Entry<ByteBuffer>> mergedData = new TreeMap<>();
    private Iterator<Map.Entry<ByteBuffer, Entry<ByteBuffer>>> mergedDataIterator = mergedData.entrySet().iterator();
    private final List<PeekIterator> iterators = new ArrayList<>();
    private final List<Entry<ByteBuffer>> lastEntries = new ArrayList<>();
    private final Lock lock = new ReentrantLock();
    private List<ByteBuffer> tombstones;

    public MergeIterator(ByteBuffer from, ByteBuffer to, ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> data,
                NavigableMap<Integer, Entry<Path>> pathsToPairedFiles, List<ByteBuffer> tombstones)
            throws IOException {
        this.tombstones = tombstones;

        ByteBuffer _from = from;
        if (from == null) {
            _from = Utils.EMPTY_BYTEBUFFER;
        }

        if (to == null) {
            iterators.add(new PeekIterator(data.tailMap(_from).values().iterator()));
        } else {
            iterators.add(new PeekIterator(data.subMap(_from, to).values().iterator()));
        }

        for (Entry<Path> entry: pathsToPairedFiles.values()) {
            iterators.add(new PeekIterator(new FileIterator(entry.key(), entry.value(), _from, to)));
        }

        //merge();
    }

    @Override
    public boolean hasNext() {
        return mergedDataIterator.hasNext() || !iterators.isEmpty() && iteratorsHaveNext();
    }

    private boolean iteratorsHaveNext() {
        boolean iteratorsHaveNext = false;
        peekAll();
        for (PeekIterator iterator: iterators) {
            iteratorsHaveNext |= iterator.hasNext();
        }
        return iteratorsHaveNext;
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
        try {
            lock.lock();
            if (mergedDataIterator.hasNext()) {
                return;
            }

            //Utils.t.refresh();
            mergedData.clear();

            Entry<ByteBuffer> curEntry = Utils.EMPTY_ENTRY;
            while (!isThresholdReached() && hasNext()) {
                iterators.removeIf(this::removeIteratorIf);

                if (iterators.size() == 1) {
                    while (hasNext() && !isThresholdReached()) {
                        curEntry = iterators.get(0).next();
                        putIfNotTombstone(curEntry);
                    }
                    mergedDataIterator = mergedData.entrySet().iterator();
                    //System.out.println("ELAPSE FOR MERGE1: " + Utils.t.elapse() + "s");
                    return;
                }

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
            //System.out.println("ELAPSE FOR MERGE2: " + Utils.t.elapse() + "s");
        } finally {
            lock.unlock();
        }
    }

    private void putIfNotTombstone(Entry<ByteBuffer> entry) {
        if (Utils.isTombstone(entry)) {
            fallEntry(entry);
        } else {
            mergedData.put(entry.key(), entry);
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
            Entry<ByteBuffer> entry = iterator.peek();
            while (iterator.hasNext() && Utils.isTombstone(entry) || entry != null && tombstones.contains(entry.key())) {
                entry = iterator.next();
            }
            if (iterator.hasNext()) {
                lastEntries.add(iterator.peek());
            }
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
