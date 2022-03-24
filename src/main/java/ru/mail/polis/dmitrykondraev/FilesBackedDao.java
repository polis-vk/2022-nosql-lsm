package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static ru.mail.polis.dmitrykondraev.MemorySegmentComparator.LEXICOGRAPHICALLY;

/**
 * Author: Dmitry Kondraev.
 */

public class FilesBackedDao implements Dao<MemorySegment, MemorySegmentEntry> {
    private final ConcurrentNavigableMap<MemorySegment, MemorySegmentEntry> map =
            new ConcurrentSkipListMap<>(LEXICOGRAPHICALLY);
    private final Deque<SortedStringTable> sortedStringTables = new ConcurrentLinkedDeque<>();
    private final Path basePath;

    public FilesBackedDao(Config config) throws IOException {
        basePath = config.basePath();
        Files
                .list(basePath)
                .filter(Files::isDirectory)
                .sorted(Comparator.reverseOrder())
                .forEachOrdered((Path subDirectory) -> {
                    if (!Files.isDirectory(subDirectory)) {
                        return;
                    }
                    sortedStringTables.add(SortedStringTable.of(subDirectory));
                });
    }

    private static <K, V> Iterator<V> iterator(Map<K, V> map) {
        return map.values().iterator();
    }

    private Iterator<MemorySegmentEntry> inMemoryGet(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return iterator(map);
        }
        if (from == null) {
            return iterator(map.headMap(to));
        }
        if (to == null) {
            return iterator(map.tailMap(from));
        }
        return iterator(map.subMap(from, to));
    }

    @Override
    public Iterator<MemorySegmentEntry> get(MemorySegment from, MemorySegment to) throws IOException {
        List<Iterator<MemorySegmentEntry>> iterators = new ArrayList<>(1 + sortedStringTables.size());
        iterators.add(inMemoryGet(from, to));
        for (SortedStringTable table : sortedStringTables) {
            iterators.add(table.get(from, to));
        }
        return new Iterator<>() {
            private final Set<MemorySegment> keys = new TreeSet<>(LEXICOGRAPHICALLY);
            private final PriorityQueue<IndexedEntry> entries;

            // IIB
            {
                entries = new PriorityQueue<>(iterators.size() + 1);
                for (int i = 0; i < iterators.size(); i++) {
                    addFrom(i);
                }
            }

            private boolean addFrom(int tableIndex) {
                Iterator<MemorySegmentEntry> iterator = iterators.get(tableIndex);
                if (!iterator.hasNext()) {
                    return false;
                }
                MemorySegmentEntry entry = iterator.next();
                return keys.add(entry.key())
                        && entry.value() != null
                        && entries.offer(new IndexedEntry(tableIndex, entry));
            }

            @Override
            public boolean hasNext() {
                if (entries.isEmpty() || iterators.isEmpty()) {
                    return false;
                }
                IndexedEntry next = entries.peek();
                if (addFrom(next.index)) {
                    return true;
                }
                for (int i = 0; i < iterators.size(); i++) {
                    if (i == next.index) {
                        continue;
                    }
                    if (addFrom(i)) {
                        break;
                    }
                }
                return true;
            }

            @Override
            public MemorySegmentEntry next() {
                IndexedEntry next = entries.poll();
                if (next == null) {
                    throw new NoSuchElementException();
                }
                return next.entry();
            }
        };
    }

    @Override
    public void upsert(MemorySegmentEntry entry) {
        // implicit check for non-null entry and entry.key()
        map.put(entry.key(), entry);
    }

    @Override
    public MemorySegmentEntry get(MemorySegment key) throws IOException {
        MemorySegmentEntry result = map.get(key);
        if (result != null) {
            if (result.value() == null) {
                return null;
            }
            return result;
        }
        for (SortedStringTable table : sortedStringTables) {
            MemorySegmentEntry entry = table.get(key);
            if (entry == null) {
                continue;
            }
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }
        return null;
    }

    @Override
    public void flush() throws IOException {
        // NOTE consider factor out format string parameter
        String directoryName = String.format("%010d", sortedStringTables.size());
        SortedStringTable.of(Files.createDirectory(basePath.resolve(directoryName)))
                .write(map.values())
                .close();
        sortedStringTables.addFirst(SortedStringTable.of(basePath.resolve(directoryName)));
        map.clear();
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private record IndexedEntry(int index, MemorySegmentEntry entry) implements Comparable<IndexedEntry> {
        @Override
        public int compareTo(IndexedEntry o) {
            int compare = LEXICOGRAPHICALLY.compare(this.entry.key(), o.entry.key());
            if (compare != 0) {
                return compare;
            }
            return Integer.compare(index, o.index);
        }
    }
}
