package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Author: Dmitry Kondraev.
 */

public class FilesBackedDao implements Dao<MemorySegment, MemorySegmentEntry> {
    private final Deque<SortedStringTable> sortedStringTables = new ArrayDeque<>();
    private final Path basePath;
    private final Path compactDir;
    private final ConcurrentNavigableMap<MemorySegment, MemorySegmentEntry> map =
            new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);

    private final static String COMPACT_NAME = "compacted";

    public FilesBackedDao(Config config) throws IOException {
        basePath = config.basePath();
        compactDir = basePath.resolve(COMPACT_NAME);
        if (Files.exists(compactDir)) {
            throw new CompactDirectoryAlreadyExistsException(compactDir);
        }
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
        if (map.isEmpty()) {
            return;
        }
        Path tablePath = sortedStringTablePath(sortedStringTables.size());
        SortedStringTable.of(Files.createDirectory(tablePath))
                .write(map.values())
                .close();
        sortedStringTables.addFirst(SortedStringTable.of(tablePath));
        map.clear();
    }

    @Override
    public void compact() throws IOException {
        SortedStringTable.of(Files.createDirectory(compactDir))
                .write(all())
                .close();
        map.clear();
        for (int i = sortedStringTables.size() - 1; i >= 0; i--) {
            Files.delete(sortedStringTablePath(i).resolve(SortedStringTable.DATA_FILENAME));
            Files.delete(sortedStringTablePath(i).resolve(SortedStringTable.INDEX_FILENAME));
            Files.delete(sortedStringTablePath(i));
        }
        sortedStringTables.clear();
        Files.move(compactDir, sortedStringTablePath(0), StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private Path sortedStringTablePath(int index) {
        return basePath.resolve(String.format("%010d", index));
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
     *
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
