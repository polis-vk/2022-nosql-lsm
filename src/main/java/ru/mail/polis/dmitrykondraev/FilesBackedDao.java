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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Author: Dmitry Kondraev.
 */

public class FilesBackedDao implements Dao<MemorySegment, MemorySegmentEntry> {
    private final ConcurrentNavigableMap<MemorySegment, MemorySegmentEntry> map =
            new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);
    private final Deque<SortedStringTable> sortedStringTables = new ConcurrentLinkedDeque<>();
    private final Path basePath;

    public FilesBackedDao(Config config) throws IOException {
        basePath = config.basePath();
        try (Stream<Path> list = Files.list(basePath)) {
            list.filter(Files::isDirectory)
                .sorted(Comparator.reverseOrder())
                .forEachOrdered((Path subDirectory) -> sortedStringTables.add(SortedStringTable.of(subDirectory)));
        }
    }

    @Override
    public Iterator<MemorySegmentEntry> get(MemorySegment from, MemorySegment to) throws IOException {
        if (from == null) {
            return get(MemorySegmentComparator.MINIMAL, to);
        }
        Iterator<MemorySegmentEntry> inMemoryIterator = inMemoryGet(from, to);
        if (sortedStringTables.isEmpty()) {
            return withoutTomStones(new PeekIterator<>(inMemoryIterator));
        }
        List<PeekIterator<MemorySegmentEntry>> iterators = new ArrayList<>(1 + sortedStringTables.size());
        iterators.add(new PeekIterator<>(inMemoryIterator));
        for (SortedStringTable table : sortedStringTables) {
            iterators.add(new PeekIterator<>(table.get(from, to)));
        }
        return withoutTomStones(new PeekIterator<>(merged(iterators)));
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
            return result.isTomStone() ? null : result;
        }
        for (SortedStringTable table : sortedStringTables) {
            MemorySegmentEntry entry = table.get(key);
            if (entry != null) {
                return entry.isTomStone() ? null : entry;
            }
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

    private static <K, V> Iterator<V> iterator(Map<K, V> map) {
        return map.values().iterator();
    }

    private Iterator<MemorySegmentEntry> inMemoryGet(MemorySegment from, MemorySegment to) {
        Map<MemorySegment, MemorySegmentEntry> subMap = to == null ? map.tailMap(from) : map.subMap(from, to);
        return iterator(subMap);
    }

    /**
     * Yields entries from multiple iterators of {@link MemorySegmentEntry}. Entries with same keys are merged,
     * leaving one entry from iterator with minimal index.
     * @param iterators which entries are strict ordered by key: key of subsequent entry is strictly greater than
     *                  key of current entry (using {@link MemorySegmentComparator})
     * @return iterator which entries are <em>also</em> strict ordered by key.
     */
    private static Iterator<MemorySegmentEntry> merged(List<PeekIterator<MemorySegmentEntry>> iterators) {
        Comparator<Integer> indexComparator = Comparator
                .comparing((Integer i) -> iterators.get(i).peek().key(), MemorySegmentComparator.INSTANCE)
                .thenComparing(Function.identity());
        final PriorityQueue<Integer> indexes = new PriorityQueue<>(iterators.size(), indexComparator);
        for (int i = 0; i < iterators.size(); i++) {
            if (iterators.get(i).hasNext()) {
                indexes.add(i);
            }
        }
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return !indexes.isEmpty();
            }

            @Override
            public MemorySegmentEntry next() {
                Integer index = indexes.remove();
                PeekIterator<MemorySegmentEntry> iterator = iterators.get(index);
                MemorySegmentEntry entry = iterator.next();
                skipEntriesWithSameKey(entry);
                if (iterator.hasNext()) {
                    indexes.offer(index);
                }
                return entry;
            }

            private void skipEntriesWithSameKey(MemorySegmentEntry entry) {
                while (!indexes.isEmpty()) {
                    Integer nextIndex = indexes.peek();
                    PeekIterator<MemorySegmentEntry> nextIterator = iterators.get(nextIndex);
                    if (MemorySegmentComparator.INSTANCE.compare(nextIterator.peek().key(), entry.key()) != 0) {
                        break;
                    }
                    indexes.remove();
                    nextIterator.next();
                    if (nextIterator.hasNext()) {
                        indexes.offer(nextIndex);
                    }
                }
            }
        };
    }

    private static Iterator<MemorySegmentEntry> withoutTomStones(PeekIterator<MemorySegmentEntry> iterator) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                while (iterator.hasNext()) {
                    if (!iterator.peek().isTomStone()) {
                        return true;
                    }
                    iterator.next();
                }
                return false;
            }

            @Override
            public MemorySegmentEntry next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return iterator.next();
            }
        };
    }
}
