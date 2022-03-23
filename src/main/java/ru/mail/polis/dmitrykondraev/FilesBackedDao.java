package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
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
            private final PriorityQueue<IndexedEntry> entries = new PriorityQueue<>();

            @Override
            public boolean hasNext() {
                if (!entries.isEmpty()) {
                    return true;
                }
                if (iterators.isEmpty()) {
                    return false;
                }
                ListIterator<Iterator<MemorySegmentEntry>> listIterator = iterators.listIterator();
                while (listIterator.hasNext()) {
                    int index = listIterator.nextIndex();
                    Iterator<MemorySegmentEntry> iterator = listIterator.next();
                    while (iterator.hasNext()) {
                        MemorySegmentEntry entry = iterator.next();
                        if (keys.add(entry.key()) && entry.value() != null) {
                            entries.offer(new IndexedEntry(index, entry));
                            break;
                        }
                    }
                }
                return !entries.isEmpty();
            }

            @Override
            public MemorySegmentEntry next() {
                IndexedEntry indexedEntry = entries.poll();
                Iterator<MemorySegmentEntry> iterator = iterators.get(indexedEntry.index);
                while (iterator.hasNext()) {
                    MemorySegmentEntry entry = iterator.next();
                    if (keys.add(entry.key()) && entry.value() != null) {
                        entries.offer(new IndexedEntry(indexedEntry.index, entry));
                        break;
                    }
                }
                return indexedEntry.entry();
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
