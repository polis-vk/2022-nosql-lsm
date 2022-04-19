package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
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

import static ru.mail.polis.dmitrykondraev.Files.filenameOf;

/**
 * Author: Dmitry Kondraev.
 */

public class ConcurrentFilesBackedDao implements Dao<MemorySegment, MemorySegmentEntry> {
    private static final String COMPACT_NAME = "compacted";
    private static final String TABLE_PREFIX = "table";
    private static final String TMP_SUFFIX = "-temp";

    private final Path basePath;
    private final Path compactDir;
    private final Path compactDirTmp;
    /**
     * ordered from most recent to the earliest.
     */
    private final Deque<SortedStringTable> sortedStringTables = new ArrayDeque<>();
    private ConcurrentNavigableMap<MemorySegment, MemorySegmentEntry> map = newMemoryTable();

    public ConcurrentFilesBackedDao(Config config) throws IOException {
        long USE_ME_PLEASE = config.flushThresholdBytes();
        basePath = config.basePath();
        compactDirTmp = basePath.resolve(COMPACT_NAME + TMP_SUFFIX);
        compactDir = basePath.resolve(COMPACT_NAME);
        try (Stream<Path> stream = Files.list(basePath)) {
            Iterator<Path> pathIterator = stream
                    .filter(subDirectory -> filenameOf(subDirectory).startsWith(TABLE_PREFIX))
                    .sorted(Comparator.comparing(ru.mail.polis.dmitrykondraev.Files::filenameOf).reversed())
                    .iterator();
            while (pathIterator.hasNext()) {
                sortedStringTables.add(SortedStringTable.of(pathIterator.next()));
            }
        }
        if (Files.exists(compactDirTmp)) {
            Files.deleteIfExists(compactDirTmp.resolve(SortedStringTable.DATA_FILENAME));
            Files.deleteIfExists(compactDirTmp.resolve(SortedStringTable.INDEX_FILENAME));
            Files.delete(compactDirTmp);
            compactImpl();
            return;
        }
        if (Files.exists(compactDir)) {
            finishCompaction();
        }
    }

    @Override
    public Iterator<MemorySegmentEntry> get(MemorySegment from, MemorySegment to) throws IOException {
        if (from == null) {
            return get(MemorySegmentComparator.MINIMAL, to);
        }
        PeekIterator<MemorySegmentEntry> inMemoryIterator = new PeekIterator<>(inMemoryGet(from, to));
        if (sortedStringTables.isEmpty()) {
            return withoutTombStones(inMemoryIterator);
        }
        List<PeekIterator<MemorySegmentEntry>> iterators = new ArrayList<>(1 + sortedStringTables.size());
        iterators.add(inMemoryIterator);
        for (SortedStringTable table : sortedStringTables) {
            iterators.add(new PeekIterator<>(table.get(from, to)));
        }
        return withoutTombStones(new PeekIterator<>(merged(iterators)));
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
            return result.isTombStone() ? null : result;
        }
        for (SortedStringTable table : sortedStringTables) {
            MemorySegmentEntry entry = table.get(key);
            if (entry != null) {
                return entry.isTombStone() ? null : entry;
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
        ResourceScope scope = ResourceScope.newConfinedScope();
        sortedStringTables.addFirst(
                SortedStringTable.written(Files.createDirectory(tablePath), map.values(), scope)
        );
        map = newMemoryTable();
    }

    @Override
    public void compact() throws IOException {
        compactImpl();
    }

    private void compactImpl() throws IOException {
        ResourceScope scope = ResourceScope.newConfinedScope();
        SortedStringTable.written(Files.createDirectory(compactDirTmp), all(), scope);
        scope.close();
        Files.move(compactDirTmp, compactDir, StandardCopyOption.ATOMIC_MOVE);
        map = newMemoryTable();
        finishCompaction();
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private void finishCompaction() throws IOException {
        for (SortedStringTable table : sortedStringTables) {
            SortedStringTable.destroyFiles(table);
        }
        sortedStringTables.clear();
        Path table0 = sortedStringTablePath(0);
        Files.move(compactDir, table0, StandardCopyOption.ATOMIC_MOVE);
        sortedStringTables.addFirst(SortedStringTable.of(table0));
    }

    private Path sortedStringTablePath(int index) {
        if (index < 0) {
            throw new IllegalArgumentException("Negative index");
        }
        // 10^10 -  > Integer.MAX_VALUE
        String value = String.valueOf(index);
        char[] zeros = new char[10 - value.length()];
        Arrays.fill(zeros, '0');
        return basePath.resolve(TABLE_PREFIX + new String(zeros) + value);
    }

    private Iterator<MemorySegmentEntry> inMemoryGet(MemorySegment from, MemorySegment to) {
        Map<MemorySegment, MemorySegmentEntry> subMap = to == null ? map.tailMap(from) : map.subMap(from, to);
        return iterator(subMap);
    }

    private static <K, V> Iterator<V> iterator(Map<K, V> map) {
        return map.values().iterator();
    }

    private static ConcurrentSkipListMap<MemorySegment, MemorySegmentEntry> newMemoryTable() {
        return new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);
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

    private static Iterator<MemorySegmentEntry> withoutTombStones(PeekIterator<MemorySegmentEntry> iterator) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                while (iterator.hasNext()) {
                    if (!iterator.peek().isTombStone()) {
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
