package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Spliterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
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

    private final BackgroundIOExecutor backgroundExecutor = new BackgroundIOExecutor();
    private final Path basePath;
    private final Path compactDir;
    private final Path compactDirTmp;
    private final long flushThresholdBytes;
    /**
     * ordered from most recent to the earliest.
     */
    private final List<SortedStringTable> sortedStringTables = new CopyOnWriteArrayList<>();
    private final AtomicReference<MemoryTable> memoryTable = new AtomicReference<>(MemoryTable.of());
    private final ResourceScope scope = ResourceScope.newSharedScope();

    private ConcurrentFilesBackedDao(Config config) {
        basePath = config.basePath();
        compactDir = basePath.resolve(COMPACT_NAME);
        compactDirTmp = basePath.resolve(COMPACT_NAME + TMP_SUFFIX);
        flushThresholdBytes = config.flushThresholdBytes();
    }

    public static ConcurrentFilesBackedDao of(Config config) throws IOException {
        ConcurrentFilesBackedDao dao = new ConcurrentFilesBackedDao(config);
        try (Stream<Path> stream = Files.list(dao.basePath)) {
            Iterator<Path> pathIterator = stream
                    .filter(subDirectory -> filenameOf(subDirectory).startsWith(TABLE_PREFIX))
                    .sorted(Comparator.comparing(ru.mail.polis.dmitrykondraev.Files::filenameOf).reversed())
                    .iterator();
            while (pathIterator.hasNext()) {
                // TODO perf
                dao.sortedStringTables.add(SortedStringTable.of(pathIterator.next(), dao.scope));
            }
        }
        if (Files.exists(dao.compactDirTmp)) {
            SortedStringTable.destroyFiles(dao.compactDirTmp);
            dao.compactImpl();
            return dao;
        }
        if (Files.exists(dao.compactDir)) {
            dao.finishCompaction();
        }
        return dao;
    }

    @Override
    public Iterator<MemorySegmentEntry> get(MemorySegment from, MemorySegment to) throws IOException {
        if (from == null) {
            return get(MemorySegmentComparator.MINIMAL, to);
        }
        MemoryTable table = memoryTable.get();
        PeekIterator<MemorySegmentEntry> inMemoryIterator = new PeekIterator<>(table.get(from, to));
        Spliterator<SortedStringTable> tableSpliterator = sortedStringTables.spliterator();
        int tablesCount = (int) tableSpliterator.getExactSizeIfKnown();
        if (tablesCount == 0 && table.previous == null) {
            return withoutTombStones(inMemoryIterator);
        }
        List<PeekIterator<MemorySegmentEntry>> iterators =
                new ArrayList<>((table.previous == null ? 1 : 2) + tablesCount);
        iterators.add(inMemoryIterator);
        if (table.previous != null) {
            iterators.add(new PeekIterator<>(table.previous.get(from, to)));
        }
        tableSpliterator.forEachRemaining(t -> iterators.add(new PeekIterator<>(t.get(from, to))));
        return withoutTombStones(new PeekIterator<>(merged(iterators)));
    }

    private Iterator<MemorySegmentEntry> allStored(Spliterator<SortedStringTable> tableSpliterator) {
        List<PeekIterator<MemorySegmentEntry>> iterators =
                new ArrayList<>((int) tableSpliterator.getExactSizeIfKnown());
        tableSpliterator.forEachRemaining(t ->
                iterators.add(new PeekIterator<>(t.get(MemorySegmentComparator.MINIMAL, null))));
        return withoutTombStones(new PeekIterator<>(merged(iterators)));
    }

    @Override
    public void upsert(MemorySegmentEntry entry) {
        long byteSizeAfter = memoryTable.get().upsert(entry);
        if (byteSizeAfter >= flushThresholdBytes) {
            backgroundExecutor.execute(this::flushImpl);
        }
    }

    @Override
    public MemorySegmentEntry get(MemorySegment key) throws IOException {
        MemorySegmentEntry result = memoryTable.get().get(key);
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
    public void flush() {
        backgroundExecutor.execute(this::flushImpl);
    }

    private void flushImpl() throws IOException {
        MemoryTable previous = memoryTable.getAndUpdate(MemoryTable::forward);
        if (previous.isEmpty()) {
            return;
        }
        Path tablePath = sortedStringTablePath(sortedStringTables.size());
        ResourceScope scope = ResourceScope.newConfinedScope();
        sortedStringTables.add(
                0,
                SortedStringTable.written(Files.createDirectory(tablePath), previous.values(), scope));
        memoryTable.getAndUpdate(MemoryTable::dropPrevious);
    }

    @Override
    public void compact() {
        backgroundExecutor.execute(this::compactImpl);
    }

    private void compactImpl() throws IOException {
        Spliterator<SortedStringTable> tableSpliterator = sortedStringTables.spliterator();
        if (tableSpliterator.getExactSizeIfKnown() == 0) {
            return;
        }
        ResourceScope scope = ResourceScope.newConfinedScope();
        SortedStringTable.written(Files.createDirectory(compactDirTmp), allStored(tableSpliterator), scope);
        scope.close();
        Files.move(compactDirTmp, compactDir, StandardCopyOption.ATOMIC_MOVE);
        finishCompaction();
    }

    @Override
    public void close() throws IOException {
        try {
            backgroundExecutor.awaitTerminationIndefinitely();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            flushImpl();
            scope.close();
        }
    }

    private void finishCompaction() throws IOException {
        for (int i = sortedStringTables.size() - 1; i >= 0; i--) {
            SortedStringTable.destroyFiles(sortedStringTablePath(i));
        }
        sortedStringTables.clear();
        Path table0 = sortedStringTablePath(0);
        Files.move(compactDir, table0, StandardCopyOption.ATOMIC_MOVE);
        sortedStringTables.add(0, SortedStringTable.of(table0, scope));
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
