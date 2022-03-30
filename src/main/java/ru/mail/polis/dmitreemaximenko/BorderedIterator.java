package ru.mail.polis.dmitreemaximenko;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class BorderedIterator implements Iterator<Entry<MemorySegment>> {
    private static final long NULL_VALUE_SIZE = -1;
    private final List<Source> sources;
    private static final Comparator<MemorySegment> comparator = new NaturalOrderComparator();

    static class Source {
        Iterator<Entry<MemorySegment>> iterator;
        Entry<MemorySegment> element;

        public Source(Iterator<Entry<MemorySegment>> iterator, Entry<MemorySegment> element) {
            this.iterator = iterator;
            this.element = element;
        }
    }

    public BorderedIterator(MemorySegment from, MemorySegment last, Iterator<Entry<MemorySegment>> iterator,
                             List<MemorySegment> logs) {
        sources = new LinkedList<>();
        if (iterator.hasNext()) {
            sources.add(new Source(iterator, iterator.next()));
        }

        if (logs != null) {
            for (int i = logs.size() - 1; i >= 0; i--) {
                Iterator<Entry<MemorySegment>> fileIterator = new FileEntryIterator(from, last, logs.get(i));
                if (fileIterator.hasNext()) {
                    sources.add(new Source(fileIterator, fileIterator.next()));
                }
            }
        }

        removeNextNullValues();
    }

    @Override
    public boolean hasNext() {
        return !sources.isEmpty();
    }

    @Override
    public Entry<MemorySegment> next() {
        Source source = peekIterator();
        if (source == null) {
            return null;
        }
        Entry<MemorySegment> result = source.element;
        moveAllIteratorsWithSuchKey(result.key());
        removeNextNullValues();
        return result;
    }

    private void removeNextNullValues() {
        Source source = peekIterator();
        while (source != null && source.element.value() == null) {
            moveAllIteratorsWithSuchKey(source.element.key());
            source = peekIterator();
        }
    }

    private Source peekIterator() {
        if (sources.isEmpty()) {
            return null;
        }

        MemorySegment minKey = sources.get(0).element.key();
        Source minSource = sources.get(0);
        for (int i = 1; i < sources.size(); i++) {
            Source source = sources.get(i);
            if (comparator.compare(source.element.key(), minKey) < 0) {
                minKey = source.element.key();
                minSource = source;
            }
        }

        return minSource;
    }

    private void moveAllIteratorsWithSuchKey(MemorySegment key) {
        if (sources.size() == 1) {
            if (sources.get(0).iterator.hasNext()) {
                sources.get(0).element = sources.get(0).iterator.next();
            } else {
                sources.remove(0);
            }
        } else {
            List<Source> toRemove = new LinkedList<>();
            for (Source source : sources) {
                if (comparator.compare(source.element.key(), key) == 0) {
                    if (source.iterator.hasNext()) {
                        source.element = source.iterator.next();
                    } else {
                        toRemove.add(source);
                    }
                }
            }
            sources.removeAll(toRemove);
        }
    }

    static class FileEntryIterator implements Iterator<Entry<MemorySegment>> {
        private long offset;
        private final MemorySegment log;
        private final MemorySegment last;
        private Entry<MemorySegment> next;
        private final long valuesAmount;

        private FileEntryIterator(MemorySegment from, MemorySegment last, MemorySegment log) {
            this.valuesAmount = MemoryAccess.getLongAtOffset(log, 0);
            offset = Long.BYTES + valuesAmount * Long.BYTES;
            this.log = log;
            this.next = nextNotLessThan(from);
            this.last = last == null ? null : MemorySegment.ofArray(last.toByteArray());
        }

        @Override
        public boolean hasNext() {
            return next != null && (last == null || comparator.compare(next.key(), last) < 0);
        }

        @Override
        public Entry<MemorySegment> next() {
            Entry<MemorySegment> result = next;
            next = nextNotLessThan(null);

            return result;
        }

        private Entry<MemorySegment> nextNotLessThan(MemorySegment other) {
            Entry<MemorySegment> result = null;
            while (offset < log.byteSize()) {
                long keySize = MemoryAccess.getLongAtOffset(log, offset);
                offset += Long.BYTES;
                long valueSize = MemoryAccess.getLongAtOffset(log, offset);
                offset += Long.BYTES;

                MemorySegment currentKey = log.asSlice(offset, keySize);
                if (other != null && comparator.compare(other, currentKey) > 0) {
                    if (valueSize == NULL_VALUE_SIZE) {
                        valueSize = 0;
                    }
                    offset += keySize + valueSize;
                } else {
                    if (valueSize == NULL_VALUE_SIZE) {
                        result = new BaseEntry<>(currentKey, null);
                    } else {
                        result = new BaseEntry<>(currentKey, log.asSlice(offset + keySize, valueSize));
                    }
                    if (valueSize == NULL_VALUE_SIZE) {
                        valueSize = 0;
                    }
                    offset += keySize + valueSize;
                    break;
                }
            }
            return result;
        }
    }
}
