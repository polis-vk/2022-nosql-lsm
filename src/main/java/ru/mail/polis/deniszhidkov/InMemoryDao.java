package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
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
    private final ExecutorService executor;
    private final Path basePath;
    private final ReadWriteLock upsertLock = new ReentrantReadWriteLock();
    private final long flushThresholdBytes;
    private volatile State state;
    private Future<?> flushResult;

    public InMemoryDao(Config config) throws IOException {
        this.basePath = config.basePath();
        this.flushThresholdBytes = config.flushThresholdBytes();
        finishCompact();
        this.state = new State(basePath);
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
        int priorityIndex = 0;
        PriorityPeekIterator storageIterator = findInMemoryStorageIteratorByRange(state.inMemory,
                from,
                to,
                priorityIndex
        );
        if (storageIterator.hasNext()) {
            iteratorsQueue.add(storageIterator);
            priorityIndex++;
        }
        PriorityPeekIterator flushingStorageIterator = findInMemoryStorageIteratorByRange(state.inFlushing,
                from,
                to,
                priorityIndex
        );
        if (flushingStorageIterator.hasNext()) {
            iteratorsQueue.add(flushingStorageIterator);
            priorityIndex++;
        }
        // FIXME?
        iteratorsQueue.addAll(getInStorageValues(priorityIndex));
        return iteratorsQueue.isEmpty() ? Collections.emptyIterator() : new MergeIterator(iteratorsQueue);
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
            // FIXME?
            for (int i = 0; i < state.getSizeOfStorage(); i++) {
                value = state.readers.get(i).findByKey(key);
                if (value != null) {
                    return value.value() == null ? null : value;
                }
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
        long entrySize = Utils.getEntrySize(entry);
        if (state.storageMemoryUsage.get() + entrySize >= flushThresholdBytes) {
            if (flushResult != null && !flushResult.isDone()) { // FIXME
                throw new IllegalStateException("Flush queue overflow");
            }
            try {
                flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        upsertLock.readLock().lock();
        try {
            state.inMemory.put(entry.key(), entry);
        } finally {
            upsertLock.readLock().unlock();
        }
        state.storageMemoryUsage.addAndGet(entrySize);
    }

    @Override
    public void flush() throws IOException, UncheckedIOException {
        if (isClosed.get()) {
            throw new IllegalStateException(DAO_CLOSED_EXCEPTION_TEXT);
        }
        State state = this.state;
        if (flushResult != null && !flushResult.isDone()) { // FIXME
            throw new IllegalStateException("Flush queue overflow");
        }
        upsertLock.writeLock().lock();
        try {
            state.inFlushing = state.inMemory;
            state.inMemory = new ConcurrentSkipListMap<>();
        } finally {
            upsertLock.writeLock().unlock();
        }
        state.storageMemoryUsage.set(0);
        flushResult = executor.submit(() -> {
            try {
                backgroundFlush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private synchronized void backgroundFlush() throws IOException {
        State state = this.state;
        state.writer.writeDAO(state.inFlushing);
        state.readers.add(0,
                new DaoReader(Utils.resolvePath(basePath, DATA_FILE_NAME, state.getSizeOfStorage()),
                        Utils.resolvePath(basePath, OFFSETS_FILE_NAME, state.getSizeOfStorage())
                )
        );
        state.writer = new DaoWriter(Utils.resolvePath(basePath, DATA_FILE_NAME, state.getSizeOfStorage()),
                Utils.resolvePath(basePath, OFFSETS_FILE_NAME, state.getSizeOfStorage())
        );
    }

    @Override
    public void compact() throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException(DAO_CLOSED_EXCEPTION_TEXT);
        }
        State state = this.state;
        if (state.readers.size() <= 1) {
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
        // FIXME?
        Queue<PriorityPeekIterator> iteratorsQueue = getInStorageValues(0);
        Iterator<BaseEntry<String>> allData = new MergeIterator(iteratorsQueue);
        int allDataSize = 0;
        while (allData.hasNext()) {
            allDataSize++;
            allData.next();
        }
        allData = new MergeIterator(iteratorsQueue);
        Path pathToTmpDataFile = Utils.resolvePath(basePath, DATA_FILE_NAME, -2);
        Path pathToTmpOffsetsFile = Utils.resolvePath(basePath, OFFSETS_FILE_NAME, -2);
        DaoWriter tmpWriter = new DaoWriter(pathToTmpDataFile, pathToTmpOffsetsFile);
        tmpWriter.writeDAOWithoutTombstones(allData, allDataSize);
        /* Если есть хотя бы один compacted файл, значит все данные были записаны.
         *  До этого не будем учитывать tmp файлы при восстановлении. */
        Files.move(pathToTmpDataFile,
                Utils.resolvePath(basePath, DATA_FILE_NAME, -1),
                StandardCopyOption.ATOMIC_MOVE
        );
        Files.move(pathToTmpOffsetsFile,
                Utils.resolvePath(basePath, OFFSETS_FILE_NAME, -1),
                StandardCopyOption.ATOMIC_MOVE
        );
        finishCompact();
        // Маркируем скомпакченные файлы, как удалённые, чтобы не читать из них и удалить их в close()
        // FIXME?
        for (int i = 0; i < state.getSizeOfStorage(); i++) {
            state.readers.get(i).setRemoved();
        }
    }

    private PriorityQueue<PriorityPeekIterator> getInStorageValues(int index) throws IOException {
        PriorityQueue<PriorityPeekIterator> iteratorsQueue = new PriorityQueue<>(
                Comparator.comparing((PriorityPeekIterator o) ->
                        o.peek().key()).thenComparingInt(PriorityPeekIterator::getPriorityIndex)
        );
        int priorityIndex = index;
        for (int i = 0; i < state.getSizeOfStorage(); i++) {
            FileIterator fileIterator = new FileIterator(null, null, state.readers.get(i));
            if (fileIterator.hasNext()) {
                iteratorsQueue.add(new PriorityPeekIterator(fileIterator, priorityIndex++));
            }
        }
        return iteratorsQueue;
    }

    private void finishCompact() throws IOException {
        State state = this.state;
        Path pathToCompactedDataFile = Utils.resolvePath(basePath, DATA_FILE_NAME, -1);
        Path pathToCompactedOffsetsFile = Utils.resolvePath(basePath, OFFSETS_FILE_NAME, -1);
        boolean isDataCompacted = Files.exists(pathToCompactedDataFile);
        boolean isOffsetsCompacted = Files.exists(pathToCompactedOffsetsFile);
        /* Если нет ни одного compacted файла, значит либо данные уже compacted, либо упали, не записав всех данных. */
        if (!isDataCompacted && !isOffsetsCompacted) {
            return;
        }
        /* Если только offsets файл compacted, то в соответствии с последовательностью на строках <> значит,
         * что мы упали между <>. Не берём lock, т. к. попадём в это условие только при аварийной ситуации */
        if (!isDataCompacted) {
            Files.move(pathToCompactedOffsetsFile,
                    Utils.resolvePath(basePath, OFFSETS_FILE_NAME, state.getSizeOfStorage()),
                    StandardCopyOption.ATOMIC_MOVE
            );
            return;
        }
        Path pathToTmpOffsetsFile = Utils.resolvePath(basePath, OFFSETS_FILE_NAME, -2);
        /* Если data файл compacted и offsets файл не compacted, значит, что не успели перевести файл offsets из tmp в
         * compacted. При этом запись полностью прошла в соответствии с последовательностью на строках <> */
        if (Files.exists(pathToTmpOffsetsFile)) {
            Files.move(pathToTmpOffsetsFile, pathToCompactedOffsetsFile, StandardCopyOption.ATOMIC_MOVE);
        }
        /* Код ниже выполнится и в том случае, если мы зайдём в данный метод из метода backgroundCompact(), поскольку
         * в таком случае у нас оба файла будут compacted. Берём lock, чтобы не произошёл конфликт с flush
         * в нумерации файлов. */
        Path pathToNewStorage = Utils.resolvePath(basePath, DATA_FILE_NAME, state.getSizeOfStorage());
        Path pathToNewOffsets = Utils.resolvePath(basePath, OFFSETS_FILE_NAME, state.getSizeOfStorage());
        Files.move(pathToCompactedDataFile, pathToNewStorage, StandardCopyOption.ATOMIC_MOVE);
        Files.move(pathToCompactedOffsetsFile, pathToNewOffsets, StandardCopyOption.ATOMIC_MOVE);
        state.readers.add(0, new DaoReader(pathToNewStorage, pathToNewOffsets));
        state.writer = new DaoWriter(Utils.resolvePath(basePath, DATA_FILE_NAME, state.getSizeOfStorage()),
                Utils.resolvePath(basePath, OFFSETS_FILE_NAME, state.getSizeOfStorage())
        );
    }

    @Override
    public synchronized void close() throws IOException { // FIXME
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
        int currentFileNumber = state.getSizeOfStorage() - 1;
        List<Integer> numbersOfFilesToRename = new ArrayList<>();
        boolean needToRename = false;
        for (DaoReader reader : state.readers) {
            boolean isRemoved = reader.getIsRemoved();
            reader.close();
            if (isRemoved) {
                Files.delete(Utils.resolvePath(basePath, DATA_FILE_NAME, currentFileNumber));
                Files.delete(Utils.resolvePath(basePath, OFFSETS_FILE_NAME, currentFileNumber--));
                needToRename = true;
            } else {
                numbersOfFilesToRename.add(currentFileNumber--);
            }
        }
        /* Если ни один из storage в памяти не был заполнен, то это означает, что никаких операций upsert или flush
         * не производилось. Если numbersOfFilesToRename тоже пуст, значит не было и компакта. Если всех вышеописанных
         * операций не производилось, то имеет смысл закончить close() здесь, чтобы не делать лишней работы
         * (все нужные ресурсы уже освобождены). */
        if (state.inMemory.isEmpty() && state.inFlushing.isEmpty() && !needToRename) {
            state.readers.clear();
            return;
        }
        /* Так как поиск файлов при инициализации DAO зависит от количества файлов, нужно, чтобы все номера файлов
         *  в названии были меньше количества файлов, поэтому необходимо переименовать файлы. */
        Collections.reverse(numbersOfFilesToRename);
        int numberOfNewFile = 0;
        for (int numberOfFile : numbersOfFilesToRename) {
            Files.move(Utils.resolvePath(basePath, DATA_FILE_NAME, numberOfFile),
                    Utils.resolvePath(basePath, DATA_FILE_NAME, numberOfNewFile),
                    StandardCopyOption.ATOMIC_MOVE
            );
            Files.move(Utils.resolvePath(basePath, OFFSETS_FILE_NAME, numberOfFile),
                    Utils.resolvePath(basePath, OFFSETS_FILE_NAME, numberOfNewFile++),
                    StandardCopyOption.ATOMIC_MOVE
            );
        }
        state.writer = new DaoWriter(Utils.resolvePath(basePath, DATA_FILE_NAME, numberOfNewFile),
                Utils.resolvePath(basePath, OFFSETS_FILE_NAME, numberOfNewFile)
        );
        state.writer.writeDAO(state.inMemory);
        state.inFlushing.clear();
        state.inMemory.clear();
        state.readers.clear();
    }

    private PriorityPeekIterator findInMemoryStorageIteratorByRange(
            ConcurrentNavigableMap<String, BaseEntry<String>> storage,
            String from,
            String to,
            int priorityIndex
    ) {
        if (from == null && to == null) {
            return new PriorityPeekIterator(storage.values().iterator(), priorityIndex);
        } else if (from == null) {
            return new PriorityPeekIterator(storage.headMap(to).values().iterator(), priorityIndex);
        } else if (to == null) {
            return new PriorityPeekIterator(storage.tailMap(from).values().iterator(), priorityIndex);
        } else {
            return new PriorityPeekIterator(storage.subMap(from, to).values().iterator(), priorityIndex);
        }
    }

    private static class State {

        private ConcurrentNavigableMap<String, BaseEntry<String>> inMemory = new ConcurrentSkipListMap<>();
        private ConcurrentNavigableMap<String, BaseEntry<String>> inFlushing = new ConcurrentSkipListMap<>();
        private final AtomicLong storageMemoryUsage = new AtomicLong(0);
        private final CopyOnWriteArrayList<DaoReader> readers;
        private DaoWriter writer;
        private final Path basePath;

        State(Path basePath) throws IOException {
            this.basePath = basePath;
            validateDAOFiles();
            this.readers = initDaoReaders();
            this.writer = new DaoWriter(Utils.resolvePath(basePath, DATA_FILE_NAME, readers.size()),
                    Utils.resolvePath(basePath, OFFSETS_FILE_NAME, readers.size())
            );
        }

        private void validateDAOFiles() throws IOException {
            int numberOfStorages = 0;
            int numberOfOffsets = 0;
            // Удаляем файлы из директории, не относящиеся к нашей DAO, и считаем количество storage
            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(basePath)) {
                for (Path file : directoryStream) {
                    String fileName = file.getFileName().toString();
                    if (fileName.startsWith(DATA_FILE_NAME)) {
                        numberOfStorages++;
                    } else if (fileName.startsWith(OFFSETS_FILE_NAME)) {
                        numberOfOffsets++;
                    } else {
                        Files.delete(file);
                    }
                }
            }
            if (numberOfStorages != numberOfOffsets) {
                throw new IllegalStateException("Number of storages and offsets didn't match");
            }
            for (int i = 0; i < numberOfStorages; i++) {
                if (!Files.exists(Utils.resolvePath(basePath, OFFSETS_FILE_NAME, i))) {
                    throw new IllegalStateException("There is no offsets file for some storage: storage number " + i);
                }
            }
        }

        private CopyOnWriteArrayList<DaoReader> initDaoReaders() throws IOException {
            CopyOnWriteArrayList<DaoReader> resultList = new CopyOnWriteArrayList<>();
            // Методом validateDaoFiles() гарантируется, что существуют все файлы по порядку от 0 до N.
            for (int i = 0; ; i--) {
                try {
                    resultList.add(new DaoReader(
                                    Utils.resolvePath(basePath, DATA_FILE_NAME, i),
                                    Utils.resolvePath(basePath, OFFSETS_FILE_NAME, i)
                            )
                    );
                } catch (FileNotFoundException e) {
                    break;
                }
            }
            Collections.reverse(resultList);
            return resultList;
        }

        public int getSizeOfStorage() {
            return readers.size();
        }
    }
}
