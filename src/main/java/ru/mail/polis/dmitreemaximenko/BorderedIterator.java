package ru.mail.polis.dmitreemaximenko;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;

public class BorderedIterator implements Iterator<Entry<MemorySegment>> {
    private static final long NULL_VALUE_SIZE = -1;
    private static final Comparator<MemorySegment> COMPARATOR = NaturalOrderComparator.getInstance();
    private final NavigableMap<MemorySegment, Source> sources;

    private static class Source {
        Iterator<Entry<MemorySegment>> iterator;
        Entry<MemorySegment> element;
        final int id;

        public Source(Iterator<Entry<MemorySegment>> iterator, Entry<MemorySegment> element, int id) {
            this.iterator = iterator;
            this.element = element;
            this.id = id;
        }
    }

    public BorderedIterator(MemorySegment from, MemorySegment last, Iterator<Entry<MemorySegment>> iterator,
                             List<MemorySegment> logs) {
        int sourceId = 0;
        sources = new TreeMap<>(COMPARATOR);
        if (iterator.hasNext()) {
            addSource(new Source(iterator, iterator.next(), sourceId));
            sourceId++;
        }

        if (logs != null) {
            for (int i = logs.size() - 1; i >= 0; i--) {
                Iterator<Entry<MemorySegment>> fileIterator = new FileEntryIterator(from, last, logs.get(i));
                if (fileIterator.hasNext()) {
                    addSource(new Source(fileIterator, fileIterator.next(), sourceId));
                    sourceId++;
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
        if (sources.size() == 1) {
            Entry<MemorySegment> result = sources.firstEntry().getValue().element;
            if (!moveSource(sources.firstEntry().getValue())) {
                sources.remove(sources.firstEntry().getKey());
            }
            removeNextNullValues();
            return result;
        }

        Source source = popIterator();
        if (source == null) {
            throw new NoSuchElementException();
        }
        Entry<MemorySegment> result = source.element;
        if (moveSource(source)) {
            addSource(source);
        }
        removeNextNullValues();
        return result;
    }

    private boolean moveSource(Source source) {
        if (source.iterator.hasNext()) {
            source.element = source.iterator.next();
            return true;
        }
        return false;
    }

    private void removeNextNullValues() {
        Source source = peekIterator();
        while (source != null && source.element.value() == null) {
            popIterator();
            if (moveSource(source)) {
                addSource(source);
            }
            source = peekIterator();
        }
    }

    private Source popIterator() {
        if (sources.isEmpty()) {
            return null;
        }

        Source minSource = sources.firstEntry().getValue();
        sources.remove(sources.firstKey());
        return minSource;
    }

    private Source peekIterator() {
        if (sources.isEmpty()) {
            return null;
        }

        return sources.firstEntry().getValue();
    }

    private void addSource(Source changedSource) {
        Source source = changedSource;
        while (true) {
            Source existedSourceWithSameKey = sources.getOrDefault(source.element.key(), null);
            if (existedSourceWithSameKey == null) {
                sources.put(source.element.key(), source);
                break;
            }

            if (existedSourceWithSameKey.id > source.id) {
                sources.put(source.element.key(), source);
                source = existedSourceWithSameKey;
            }
            if (!source.iterator.hasNext()) {
                break;
            }
            source.element = source.iterator.next();
        }
    }

    static class FileEntryIterator implements Iterator<Entry<MemorySegment>> {
        private long offset;
        private final MemorySegment log;
        private final MemorySegment last;
        private Entry<MemorySegment> next;
        private final long valuesAmount;

        private class EntryContainer {
            Entry<MemorySegment> entry;
            long entrySize;
        }

        private FileEntryIterator(MemorySegment from, MemorySegment last, MemorySegment log) {
            this.log = log;
            if (log.byteSize() > 0) {
                valuesAmount = MemoryAccess.getLongAtOffset(log, 0);
                if (valuesAmount > 0) {
                    offset = getOffsetOfEntryNotLessThan(from);
                    if (offset < 0) {
                        offset = log.byteSize();
                    } else {
                        next = getEntryByOffset();
                    }
                } else {
                    offset = log.byteSize();
                }
            } else {
                valuesAmount = 0;
                offset = log.byteSize();
            }
            this.last = last == null ? null : MemorySegment.ofArray(last.toByteArray());
        }

        @Override
        public boolean hasNext() {
            return next != null && (last == null || COMPARATOR.compare(next.key(), last) < 0);
        }

        @Override
        public Entry<MemorySegment> next() {
            Entry<MemorySegment> result = next;
            EntryContainer nextEntry = getNextEntry();
            offset += nextEntry.entrySize;
            next = nextEntry.entry;
            
            return result;
        }

        private EntryContainer getNextEntry() {
            EntryContainer result = new EntryContainer();
            long entryOffset = offset;
            if (entryOffset < log.byteSize()) {
                long keySize = MemoryAccess.getLongAtOffset(log, entryOffset);
                entryOffset += Long.BYTES;
                long valueSize = MemoryAccess.getLongAtOffset(log, entryOffset);
                entryOffset += Long.BYTES;

                MemorySegment currentKey = log.asSlice(entryOffset, keySize);
                if (valueSize == NULL_VALUE_SIZE) {
                    result.entry = new BaseEntry<>(currentKey, null);
                } else {
                    result.entry = new BaseEntry<>(currentKey, log.asSlice(entryOffset + keySize,
                            valueSize));
                }
                if (valueSize == NULL_VALUE_SIZE) {
                    valueSize = 0;
                }
                entryOffset += keySize + valueSize;

            }

            result.entrySize = entryOffset - offset;
            return result;
        }

        private long getOffsetOfEntryNotLessThan(MemorySegment other) {
            long low = 0;
            long high = valuesAmount - 1;

            long result = -1;
            while (low <= high) {
                long mid = (low + high) >>> 1;
                MemorySegment midVal = getKeyByIndex(mid);
                int cmp = COMPARATOR.compare(midVal, other);

                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                    result = mid;
                } else {
                    return getEntryOffsetByIndex(mid);
                }
            }

            return result == - 1 ? result : getEntryOffsetByIndex(result);
        }

        private MemorySegment getKeyByIndex(long index) {
            long entryOffset = getEntryOffsetByIndex(index);

            long keySize = MemoryAccess.getLongAtOffset(log, entryOffset);
            long keyOffset = entryOffset + 2L * Long.BYTES;
            return log.asSlice(keyOffset, keySize);
        }

        private Entry<MemorySegment> getEntryByOffset() {
            long keySize = MemoryAccess.getLongAtOffset(log, offset);
            long valueSize = MemoryAccess.getLongAtOffset(log, offset + Long.BYTES);

            long keyOffset = offset + 2L * Long.BYTES;
            MemorySegment key = log.asSlice(keyOffset, keySize);

            long valueOffset = keyOffset + keySize;
            if (valueSize == NULL_VALUE_SIZE) {
                offset = valueOffset;
                return new BaseEntry<>(key, null);
            }

            MemorySegment value = log.asSlice(valueOffset, valueSize);

            offset = valueOffset + valueSize;
            return new BaseEntry<>(key, value);
        }

        private long getEntryOffsetByIndex(long index) {
            long indexOffset = Long.BYTES + index * Long.BYTES;
            return MemoryAccess.getLongAtOffset(log, indexOffset);
        }
    }
}
