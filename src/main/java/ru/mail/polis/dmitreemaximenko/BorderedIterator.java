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
    static private final Comparator<MemorySegment> comparator = new NaturalOrderComparator();;
    static class Source {
        Iterator<Entry<MemorySegment>> iterator;
        Entry<MemorySegment> element;

        public Source(Iterator<Entry<MemorySegment>> iterator, Entry<MemorySegment> element) {
            this.iterator = iterator;
            this.element = element;
        }
    }

    public BorderedIterator(MemorySegment from, MemorySegment last, Iterator<Entry<MemorySegment>> iterator,
                             List<MemorySegment> readPages) {
        sources = new LinkedList<>();
        if (iterator.hasNext()) {
            sources.add(new Source(iterator, iterator.next()));
        }

        for (int i = readPages.size() - 1; i >= 0; i--) {
            Iterator<Entry<MemorySegment>> fileIterator = new FileEntryIterator(from, last, readPages.get(i));
            if (fileIterator.hasNext()) {
                sources.add(new Source(fileIterator, fileIterator.next()));
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
        for (Source source : sources) {
            if (comparator.compare(source.element.key(), minKey) < 0) {
                minKey = source.element.key();
                minSource = source;
            }
        }

        return minSource;
    }

    private void moveAllIteratorsWithSuchKey(MemorySegment key) {
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

    static class FileEntryIterator implements Iterator<Entry<MemorySegment>> {
        private long offset;
        private MemorySegment log;
        private final MemorySegment last;
        private BaseEntry<MemorySegment> next = null;

        private FileEntryIterator(MemorySegment from, MemorySegment last, MemorySegment log) {
            offset = 0;
            this.log = log;
            while (offset < log.byteSize()) {
                long keySize = MemoryAccess.getLongAtOffset(log, offset);
                offset += Long.BYTES;
                long valueSize = MemoryAccess.getLongAtOffset(log, offset);
                offset += Long.BYTES;

                MemorySegment currentKey = log.asSlice(offset, keySize);
                if (comparator.compare(from, currentKey) > 0) {
                    if (valueSize == NULL_VALUE_SIZE) {
                        valueSize = 0;
                    }
                    offset += keySize + valueSize;
                } else {
                    if (valueSize != NULL_VALUE_SIZE) {
                        next = new BaseEntry<>(currentKey, log.asSlice(offset + keySize, valueSize));
                    } else {
                        next = new BaseEntry<>(currentKey, null);
                    }
                    if (valueSize == NULL_VALUE_SIZE) {
                        valueSize = 0;
                    }
                    offset += keySize + valueSize;
                    break;
                }

            }

            this.last = last == null ? null : MemorySegment.ofArray(last.toByteArray());
        }

        @Override
        public boolean hasNext() {
            return next != null && (last == null || comparator.compare(next.key(), last) < 0);
        }

        @Override
        public Entry<MemorySegment> next() {
            Entry<MemorySegment> result = next;
            next = null;

            if (offset < log.byteSize()) {
                long keySize = MemoryAccess.getLongAtOffset(log, offset);
                offset += Long.BYTES;
                long valueSize = MemoryAccess.getLongAtOffset(log, offset);
                offset += Long.BYTES;

                MemorySegment currentKey = log.asSlice(offset, keySize);
                if (valueSize != NULL_VALUE_SIZE) {
                    next = new BaseEntry<>(currentKey, log.asSlice(offset + keySize, valueSize));
                } else {
                    next = new BaseEntry<>(currentKey, null);
                }
                if (valueSize == NULL_VALUE_SIZE) {
                    valueSize = 0;
                }
                offset += keySize + valueSize;
            }
            return result;
        }
    }
}
