package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private static final String DATA_FILE_NAME = "storage";
    private static final String DAO_CLOSED_EXCEPTION_TEXT = "DAO has been closed"; // Требование CodeClimate
    private static final String OFFSETS_FILE_NAME = "offsets";
    private final AtomicBoolean isClosed = new AtomicBoolean(true);
    private final DaoUtils utils;
    private final ExecutorService executor;
    private final ReadWriteLock upsertLock = new ReentrantReadWriteLock();
    private final long flushThresholdBytes;
    private volatile State state;
    private Future<?> flushResult;

    public InMemoryDao(Config config) throws IOException {
        this.flushThresholdBytes = config.flushThresholdBytes();
        this.utils = new DaoUtils(config.basePath());
        finishCompact();
        utils.validateDAOFiles();
        CopyOnWriteArrayList<DaoReader> readers = utils.initDaoReaders();
        DaoWriter writer = new DaoWriter(utils.resolvePath(DATA_FILE_NAME, readers.size()),
                utils.resolvePath(OFFSETS_FILE_NAME, readers.size())
        );
        this.state = State.newState(readers, writer);
        this.executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "DaoBackgroundThread"));
        this.isClosed.compareAndSet(true, false);
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException(DAO_CLOSED_EXCEPTION_TEXT);
        }
        State state = this.state;
        Queue<PriorityPeekIterator> iteratorsQueue = new PriorityQueue<>(
                Comparator.comparing((PriorityPeekIterator o) ->
                        o.peek().key()).thenComparingInt(PriorityPeekIterator::getPriorityIndex)
        );
        // Если закроются readers заново вызовем get
        try {
            addIterators(iteratorsQueue, state, from, to);
        } catch (IllegalStateException e) {
            iteratorsQueue.clear();
            addIterators(iteratorsQueue, state, from, to);
        }
        return iteratorsQueue.isEmpty() ? Collections.emptyIterator() : new MergeIterator(iteratorsQueue);
    }

    private void addIterators(Queue<PriorityPeekIterator> iteratorsQueue,
                              State state,
                              String from,
                              String to
    ) throws IOException {
        int priorityIndex = 0;
        priorityIndex = addInMemoryIteratorByRange(iteratorsQueue, state.inMemory, from, to, priorityIndex);
        priorityIndex = addInMemoryIteratorByRange(iteratorsQueue, state.inFlushing, from, to, priorityIndex);
        addInStorageIteratorsByRange(iteratorsQueue, state, from, to, priorityIndex);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException(DAO_CLOSED_EXCEPTION_TEXT);
        }
        State state = this.state;
        BaseEntry<String> value = state.inMemory.get(key);
        if (value == null) {
            value = state.inFlushing.get(key);
            if (value != null) {
                return value;
            }
            // Если закроются readers снова вызовем get
            try {
                for (int i = 0; i < state.getSizeOfStorage(); i++) {
                    value = state.readers.get(i).findByKey(key);
                    if (value != null) {
                        return value.value() == null ? null : value;
                    }
                }
            } catch (IllegalStateException e) {
                get(key);
            }
            value = new BaseEntry<>(null, null);
        }
        return value.value() == null ? null : value;
    }

    @Override
    public void upsert(BaseEntry<String> entry) throws UncheckedIOException {
        if (isClosed.get()) {
            throw new IllegalStateException(DAO_CLOSED_EXCEPTION_TEXT);
        }
        State state = this.state;
        long entrySize = DaoUtils.getEntrySize(entry);
        upsertLock.readLock().lock();
        try {
            state.inMemory.put(entry.key(), entry);
            state.storageMemoryUsage.addAndGet(entrySize);
        } finally {
            upsertLock.readLock().unlock();
        }
        if (state.storageMemoryUsage.get() >= flushThresholdBytes) {
            flushResult = executor.submit(() -> {
                try {
                    backgroundFlush();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    @Override
    public synchronized void flush() throws IOException, UncheckedIOException {
        if (isClosed.get()) {
            throw new IllegalStateException(DAO_CLOSED_EXCEPTION_TEXT);
        }
        if (isFlushed()) {
            throw new IllegalStateException("Flush queue overflow");
        }

        flushResult = executor.submit(() -> {
            try {
                backgroundFlush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private synchronized boolean isFlushed() {
        return flushResult != null && !flushResult.isDone();
    }

    private void backgroundFlush() throws IOException {
        State state = this.state;

        upsertLock.writeLock().lock();
        try {
            this.state = state.beforeFlush();
        } finally {
            upsertLock.writeLock().unlock();
        }

        state.writer.writeDAO(state.inFlushing);
        state.readers.add(0, new DaoReader(utils.resolvePath(DATA_FILE_NAME, state.getSizeOfStorage() - 1),
                utils.resolvePath(OFFSETS_FILE_NAME, state.getSizeOfStorage() - 1))
        );
        DaoWriter writer = new DaoWriter(utils.resolvePath(DATA_FILE_NAME, state.getSizeOfStorage()),
                utils.resolvePath(OFFSETS_FILE_NAME, state.getSizeOfStorage())
        );

        upsertLock.writeLock().lock();
        try {
            this.state = state.afterFlush(writer);
        } finally {
            upsertLock.writeLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException(DAO_CLOSED_EXCEPTION_TEXT);
        }
        State state = this.state;
        if (state.getSizeOfStorage() <= 1) {
            return;
        }
        executor.submit(() -> {
            try {
                backgroundCompact();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private synchronized void backgroundCompact() throws IOException {
        State state = this.state;
        Queue<PriorityPeekIterator> iteratorsQueue = new PriorityQueue<>(
                Comparator.comparing((PriorityPeekIterator o) ->
                        o.peek().key()).thenComparingInt(PriorityPeekIterator::getPriorityIndex)
        );
        // Во время compact ничего сфлашиться не может, поэтому количество reader неизменно
        addInStorageIteratorsByRange(iteratorsQueue, state, null, null, 0);
        Iterator<BaseEntry<String>> allData = new MergeIterator(iteratorsQueue);
        int allDataSize = 0;
        while (allData.hasNext()) {
            allDataSize++;
            allData.next();
        }
        addInStorageIteratorsByRange(iteratorsQueue, state, null, null, 0);
        allData = new MergeIterator(iteratorsQueue);
        Path pathToTmpData = utils.resolvePath(DATA_FILE_NAME, -2);
        Path pathToTmpOffsets = utils.resolvePath(OFFSETS_FILE_NAME, -2);
        DaoWriter tmpWriter = new DaoWriter(pathToTmpData, pathToTmpOffsets);
        tmpWriter.writeDAOWithoutTombstones(allData, allDataSize);
        /* Если есть хотя бы один compacted файл, значит все данные были записаны.
         *  До этого не будем учитывать tmp файлы при восстановлении. */
        Files.move(pathToTmpData, utils.resolvePath(DATA_FILE_NAME, -1), StandardCopyOption.ATOMIC_MOVE);
        Files.move(pathToTmpOffsets, utils.resolvePath(OFFSETS_FILE_NAME, -1), StandardCopyOption.ATOMIC_MOVE);

        utils.closeReaders(state.readers);
        finishCompact();
        CopyOnWriteArrayList<DaoReader> readers = utils.initDaoReaders();
        DaoWriter writer = new DaoWriter(utils.resolvePath(DATA_FILE_NAME, readers.size()),
                utils.resolvePath(OFFSETS_FILE_NAME, readers.size())
        );

        // Нужен readLock, чтобы не блокировать upsert
        upsertLock.readLock().lock();
        try {
            this.state = state.afterCompact(readers, writer);
        } finally {
            upsertLock.readLock().unlock();
        }
    }

    private void finishCompact() throws IOException {
        Path pathToCompactedDataFile = utils.resolvePath(DATA_FILE_NAME, -1);
        Path pathToCompactedOffsetsFile = utils.resolvePath(OFFSETS_FILE_NAME, -1);
        boolean isDataCompacted = Files.exists(pathToCompactedDataFile);
        boolean isOffsetsCompacted = Files.exists(pathToCompactedOffsetsFile);
        /* Если нет ни одного compacted файла, значит либо данные уже compacted, либо упали, не записав всех данных. */
        if (!isDataCompacted && !isOffsetsCompacted) {
            return;
        }
        /* Если только offsets файл compacted, то в соответствии с последовательностью на строках <> значит,
         * что мы упали между <>. */
        if (!isDataCompacted) {
            Files.move(pathToCompactedOffsetsFile,
                    utils.resolvePath(OFFSETS_FILE_NAME, 0),
                    StandardCopyOption.ATOMIC_MOVE
            );
            return;
        }
        Path pathToTmpOffsetsFile = utils.resolvePath(OFFSETS_FILE_NAME, -2);
        /* Если data файл compacted и offsets файл не compacted, значит, что не успели перевести файл offsets из tmp в
         * compacted. При этом запись полностью прошла в соответствии с последовательностью на строках <> */
        if (Files.exists(pathToTmpOffsetsFile)) {
            Files.move(pathToTmpOffsetsFile, pathToCompactedOffsetsFile, StandardCopyOption.ATOMIC_MOVE);
        }
        /* Код ниже выполнится и в том случае, если мы зайдём в данный метод из метода backgroundCompact(), поскольку
         * в таком случае у нас оба файла будут compacted. */
        utils.removeOldFiles();
        Path pathToNewStorage = utils.resolvePath(DATA_FILE_NAME, 0);
        Path pathToNewOffsets = utils.resolvePath(OFFSETS_FILE_NAME, 0);
        Files.move(pathToCompactedDataFile, pathToNewStorage, StandardCopyOption.ATOMIC_MOVE);
        Files.move(pathToCompactedOffsetsFile, pathToNewOffsets, StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public synchronized void close() throws IOException {
        if (isClosed.get()) {
            return;
        }
        isClosed.set(true);
        executor.shutdown();
        try {
            //noinspection StatementWithEmptyBody
            while (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) ;
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        State state = this.state;
        utils.closeReaders(state.readers);
        state.readers.clear();
        if (!state.inMemory.isEmpty()) {
            state.writer.writeDAO(state.inMemory);
            state.inMemory.clear();
        }
    }

    private void addInStorageIteratorsByRange(Queue<PriorityPeekIterator> iteratorsQueue,
                                              State state,
                                              String from,
                                              String to,
                                              int index
    ) throws IOException {
        int priorityIndex = index;
        for (int i = 0; i < state.getSizeOfStorage(); i++) {
            FileIterator fileIterator = new FileIterator(from, to, state.readers.get(i));
            if (fileIterator.hasNext()) {
                iteratorsQueue.add(new PriorityPeekIterator(fileIterator, priorityIndex++));
            }
        }
    }

    private int addInMemoryIteratorByRange(Queue<PriorityPeekIterator> iteratorsQueue,
                                           ConcurrentNavigableMap<String, BaseEntry<String>> storage,
                                           String from,
                                           String to,
                                           int priorityIndex
    ) {
        PriorityPeekIterator resIterator;
        if (from == null && to == null) {
            resIterator = new PriorityPeekIterator(storage.values().iterator(), priorityIndex);
        } else if (from == null) {
            resIterator = new PriorityPeekIterator(storage.headMap(to).values().iterator(), priorityIndex);
        } else if (to == null) {
            resIterator = new PriorityPeekIterator(storage.tailMap(from).values().iterator(), priorityIndex);
        } else {
            resIterator = new PriorityPeekIterator(storage.subMap(from, to).values().iterator(), priorityIndex);
        }
        if (resIterator.hasNext()) {
            iteratorsQueue.add(resIterator);
            return ++priorityIndex;
        }
        return priorityIndex;
    }

    private static class State {

        private final AtomicLong storageMemoryUsage = new AtomicLong(0);
        private final ConcurrentNavigableMap<String, BaseEntry<String>> inMemory;
        private final ConcurrentNavigableMap<String, BaseEntry<String>> inFlushing;
        private final CopyOnWriteArrayList<DaoReader> readers;
        private final DaoWriter writer;

        State(ConcurrentNavigableMap<String, BaseEntry<String>> inMemory,
              ConcurrentNavigableMap<String, BaseEntry<String>> inFlushing,
              CopyOnWriteArrayList<DaoReader> readers,
              DaoWriter writer
        ) {
            this.inMemory = inMemory;
            this.inFlushing = inFlushing;
            this.readers = readers;
            this.writer = writer;
        }

        static State newState(CopyOnWriteArrayList<DaoReader> readers, DaoWriter writer) {
            return new State(new ConcurrentSkipListMap<>(), new ConcurrentSkipListMap<>(), readers, writer);
        }

        State beforeFlush() {
            storageMemoryUsage.set(0);
            return new State(new ConcurrentSkipListMap<>(), inMemory, readers, writer);
        }

        State afterFlush(DaoWriter writer) {
            return new State(inMemory, new ConcurrentSkipListMap<>(), readers, writer);
        }

        State afterCompact(CopyOnWriteArrayList<DaoReader> readers, DaoWriter writer) {
            return new State(inMemory, inFlushing, readers, writer);
        }

        int getSizeOfStorage() {
            return readers.size();
        }
    }
}
