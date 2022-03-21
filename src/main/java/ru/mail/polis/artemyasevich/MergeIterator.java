package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public class MergeIterator implements Iterator<BaseEntry<String>> {
    private static final String DATA_FILE = "data";
    private static final String FILE_EXTENSION = ".txt";
    private final PriorityQueue<PeekIterator> queue;
    private String keyToSkip;
    private BaseEntry<String> next;

    public MergeIterator(String from, String to, int numberOfFiles, Iterator<BaseEntry<String>> dataMapIterator,
                         Path pathToDirectory, List<long[]> offsets) throws IOException {
        this.queue = new PriorityQueue<>();
        if (dataMapIterator.hasNext()) {
            queue.add(new PeekIterator(dataMapIterator, 0));
        }
        for (int fileNumber = 0; fileNumber < numberOfFiles; fileNumber++) {
            Path path = pathToDirectory.resolve(DATA_FILE + fileNumber + FILE_EXTENSION);
            Iterator<BaseEntry<String>> fileIterator = new FileIterator(from, to, path, offsets.get(fileNumber));
            if (fileIterator.hasNext()) {
                queue.add(new PeekIterator(fileIterator, numberOfFiles - fileNumber));
            }
        }
        next = getNext();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public BaseEntry<String> next() {
        BaseEntry<String> nextToGive = next;
        next = getNext();
        return nextToGive;
    }

    private BaseEntry<String> getNext() {
        BaseEntry<String> desiredNext = null;
        while (!queue.isEmpty() && desiredNext == null) {
            PeekIterator current = queue.poll();
            desiredNext = current.next();
            if (desiredNext.value() == null || desiredNext.key().equals(keyToSkip)) {
                if (desiredNext.value() == null) {
                    keyToSkip = desiredNext.key();
                }
                if (current.hasNext()) {
                    queue.add(current);
                }
                desiredNext = null;
                continue;
            }
            keyToSkip = desiredNext.key();
            if (current.hasNext()) {
                queue.add(current);
            }
        }
        return desiredNext;
    }

    private static class PeekIterator implements Iterator<BaseEntry<String>>, Comparable<PeekIterator> {
        private final int sourceNumber;
        private final Iterator<BaseEntry<String>> delegate;
        private BaseEntry<String> peeked;

        PeekIterator(Iterator<BaseEntry<String>> iterator, int sourceNumber) {
            this.sourceNumber = sourceNumber;
            this.delegate = iterator;
        }

        @Override
        public boolean hasNext() {
            return peeked != null || delegate.hasNext();
        }

        @Override
        public BaseEntry<String> next() {
            BaseEntry<String> temp = peek();
            peeked = null;
            return temp;
        }

        public BaseEntry<String> peek() {
            if (peeked == null) {
                peeked = delegate.next();
            }
            return peeked;
        }

        @Override
        public int compareTo(PeekIterator o) {
            int keyCompare = this.peek().key().compareTo(o.peek().key());
            if (keyCompare != 0) {
                return keyCompare;
            }
            return this.sourceNumber - o.sourceNumber;
        }
    }
}
