package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private static final String DATA_FILE_NAME = "storage";
    private static final String OFFSETS_FILE_NAME = "offsets";
    private static final String TMP_QUALIFIER = "tmp_";
    private static final String COMPACTED_QUALIFIER = "compacted_";
    private static final String FILE_EXTENSION = ".txt";
    private final AtomicBoolean isClosed = new AtomicBoolean(true);
    private final AtomicInteger storagesCounter;
    private final AtomicLong storageMemoryUsage = new AtomicLong(0);
    private final ExecutorService flushExecutor;
    private final ExecutorService compactExecutor;
    private final List<DaoReader> readers;
    private final Path directoryPath;
    private final ReadWriteLock getsLock = new ReentrantReadWriteLock();
    private final ReadWriteLock flushLock = new ReentrantReadWriteLock();
    private final long flushThresholdBytes;
    private ConcurrentNavigableMap<String, BaseEntry<String>> storage = new ConcurrentSkipListMap<>();
    private ConcurrentNavigableMap<String, BaseEntry<String>> flushingStorage = new ConcurrentSkipListMap<>();
    private DaoWriter writer;
    private Future<?> flushResult;

    public InMemoryDao(Config config) throws IOException {
        this.directoryPath = config.basePath();
        this.flushThresholdBytes = config.flushThresholdBytes();
        this.flushExecutor = Executors.newSingleThreadExecutor();
        this.compactExecutor = Executors.newSingleThreadExecutor();
        finishCompact();
        this.storagesCounter = new AtomicInteger(validateDAOFiles());
        this.readers = initDaoReaders();
        this.writer = new DaoWriter(getStoragePath(storagesCounter.get()), getOffsetsPath(storagesCounter.get()));
        this.isClosed.set(false);
        BlockingMergeIterator.freeIterators();
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("DAO has been closed");
        }
        Queue<PriorityPeekIterator> iteratorsQueue = new PriorityQueue<>(
                Comparator.comparing((PriorityPeekIterator o) ->
                        o.peek().key()).thenComparingInt(PriorityPeekIterator::getPriorityIndex)
        );
        getsLock.readLock().lock();
        try {
            int priorityIndex = 0;
            PriorityPeekIterator storageIterator = findInMemoryStorageIteratorByRange(
                    storage,
                    from,
                    to,
                    priorityIndex
            );
            if (storageIterator.hasNext()) {
                iteratorsQueue.add(storageIterator);
                priorityIndex++;
            }
            PriorityPeekIterator flushingStorageIterator = findInMemoryStorageIteratorByRange(
                    flushingStorage,
                    from,
                    to,
                    priorityIndex
            );
            if (flushingStorageIterator.hasNext()) {
                iteratorsQueue.add(flushingStorageIterator);
                priorityIndex++;
            }
            for (int i = 0; i < storagesCounter.get(); i++) {
                FileIterator fileIterator = new FileIterator(from, to, readers.get(i));
                if (fileIterator.hasNext()) {
                    iteratorsQueue.add(new PriorityPeekIterator(fileIterator, priorityIndex++));
                }
            }
        } finally {
            getsLock.readLock().unlock();
        }
        return iteratorsQueue.isEmpty() ? Collections.emptyIterator() : new BlockingMergeIterator(iteratorsQueue);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("DAO has been closed");
        }
        BaseEntry<String> value = storage.get(key);
        if (value == null) {
            value = flushingStorage.get(key);
            if (value != null) {
                return value;
            }
            getsLock.readLock().lock();
            try {
                for (int i = 0; i < storagesCounter.get(); i++) {
                    value = readers.get(i).findByKey(key);
                    if (value != null) {
                        return value.value() == null ? null : value;
                    }
                }
                value = new BaseEntry<>(null, null);
            } finally {
                getsLock.readLock().unlock();
            }
        }
        return value.value() == null ? null : value;
    }

    @Override
    public void upsert(BaseEntry<String> entry) throws UncheckedIOException {
        if (isClosed.get()) {
            throw new IllegalStateException("DAO has been closed");
        }
        long entrySize = getEntrySize(entry);
        if (storageMemoryUsage.get() + entrySize >= flushThresholdBytes) {
            if (flushResult != null && !flushResult.isDone()) {
                throw new IllegalStateException("Flush queue overflow");
            }
            try {
                flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        getsLock.writeLock().lock();
        try {
            storage.put(entry.key(), entry);
        } finally {
            getsLock.writeLock().unlock();
        }
        storageMemoryUsage.addAndGet(entrySize);
    }

    private long getEntrySize(BaseEntry<String> entry) {
        return entry.value() == null
                ? entry.key().length() * 2L
                : (entry.key().length() + entry.value().length()) * 2L;
    }

    @Override
    public void flush() throws IOException, UncheckedIOException {
        if (isClosed.get()) {
            throw new IllegalStateException("DAO has been closed");
        }
        if (flushResult != null && !flushResult.isDone()) {
            throw new IllegalStateException("Flush queue overflow");
        }
        getsLock.writeLock().lock();
        try {
            flushingStorage = storage;
            storage = new ConcurrentSkipListMap<>();
        } finally {
            getsLock.writeLock().unlock();
        }
        storageMemoryUsage.set(0);
        flushResult = flushExecutor.submit(() -> {
            try {
                backgroundFlush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private void backgroundFlush() throws IOException {
        flushLock.writeLock().lock();
        try {
            writer.writeDAO(flushingStorage);
            readers.add(0, new DaoReader(getStoragePath(storagesCounter.get()), getOffsetsPath(storagesCounter.get())));
            storagesCounter.incrementAndGet();
            writer = new DaoWriter(getStoragePath(storagesCounter.get()), getOffsetsPath(storagesCounter.get()));
        } finally {
            flushLock.writeLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("DAO has been closed");
        }
        if (readers.size() <= 1) {
            return;
        }
        compactExecutor.submit(() -> {
            try {
                backgroundCompact();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private void backgroundCompact() throws IOException {
        Queue<PriorityPeekIterator> iteratorsQueue = new PriorityQueue<>(
                Comparator.comparing((PriorityPeekIterator o) ->
                        o.peek().key()).thenComparingInt(PriorityPeekIterator::getPriorityIndex)
        );
        int priorityIndex = 0;
        int lastConsiderFile = -1; // Всегда будет хотя бы два файла для компакта, если зашли в этот метод
        int startCompactFilesAmount = storagesCounter.get();
        for (int i = 0; i < startCompactFilesAmount; i++) {
            FileIterator fileIterator = new FileIterator(null, null, readers.get(i));
            if (fileIterator.hasNext()) {
                iteratorsQueue.add(new PriorityPeekIterator(fileIterator, priorityIndex++));
                lastConsiderFile = i;
            }
        }
        Iterator<BaseEntry<String>> allData = new BlockingMergeIterator(iteratorsQueue);
        int allDataSize = 0;
        while (allData.hasNext()) {
            allDataSize++;
            allData.next();
        }
        allData = new BlockingMergeIterator(iteratorsQueue);
        Path pathToTmpDataFile = getStoragePath(TMP_QUALIFIER);
        Path pathToTmpOffsetsFile = getOffsetsPath(TMP_QUALIFIER);

        DaoWriter tmpWriter = new DaoWriter(pathToTmpDataFile, pathToTmpOffsetsFile);
        tmpWriter.writeDAOWithoutTombstones(allData, allDataSize);
        /* Если есть хотя бы один compacted файл, значит все данные были записаны.
         *  До этого не будем учитывать tmp файлы при восстановлении. */
        Files.move(pathToTmpDataFile, getStoragePath(COMPACTED_QUALIFIER), StandardCopyOption.ATOMIC_MOVE);
        Files.move(pathToTmpOffsetsFile, getOffsetsPath(COMPACTED_QUALIFIER), StandardCopyOption.ATOMIC_MOVE);
        finishCompact();
        // Маркируем скомпакченные файлы, как удалённые, чтобы не читать из них и удалить их в close()
        flushLock.writeLock().lock();
        try {
            for (int i = storagesCounter.get() - startCompactFilesAmount; i < lastConsiderFile; i++) {
                readers.get(i).setRemoved();
            }
        } finally {
            flushLock.writeLock().unlock();
        }
    }

    private void finishCompact() throws IOException {
        Path pathToCompactedDataFile = getStoragePath(COMPACTED_QUALIFIER);
        Path pathToCompactedOffsetsFile = getOffsetsPath(COMPACTED_QUALIFIER);

        boolean isDataCompacted = Files.exists(pathToCompactedDataFile);
        boolean isOffsetsCompacted = Files.exists(pathToCompactedOffsetsFile);
        /* Если нет ни одного compacted файла, значит либо данные уже compacted, либо упали, не записав всех данных. */
        if (!isDataCompacted && !isOffsetsCompacted) {
            return;
        }
        /* Если только offsets файл compacted, то в соответствии с последовательностью на строках <> значит,
         * что мы упали между <>. Не берём lock, т. к. попадём в это условие только при аварийной ситуации */
        if (!isDataCompacted) {
            Files.move(pathToCompactedOffsetsFile, getOffsetsPath(storagesCounter.get()), StandardCopyOption.ATOMIC_MOVE);
            return;
        }
        Path pathToTmpOffsetsFile = getOffsetsPath(TMP_QUALIFIER);
        /* Если data файл compacted и offsets файл не compacted, значит, что не успели перевести файл offsets из tmp в
         * compacted. При этом запись полностью прошла в соответствии с последовательностью на строках <> */
        if (Files.exists(pathToTmpOffsetsFile)) {
            Files.move(pathToTmpOffsetsFile, pathToCompactedOffsetsFile, StandardCopyOption.ATOMIC_MOVE);
        }
        /* Код ниже выполнится и в том случае, если мы зайдём в данный метод из метода backgroundCompact(), поскольку
         * в таком случае у нас оба файла будут compacted. Берём lock, чтобы не произошёл конфликт с flush
         * в нумерации файлов. */
        flushLock.writeLock().lock();
        try {
            Path pathToNewStorage = getStoragePath(storagesCounter.get());
            Path pathToNewOffsets = getOffsetsPath(storagesCounter.get());
            Files.move(pathToCompactedDataFile, pathToNewStorage, StandardCopyOption.ATOMIC_MOVE);
            Files.move(pathToCompactedOffsetsFile, pathToNewOffsets, StandardCopyOption.ATOMIC_MOVE);
            readers.add(0, new DaoReader(pathToNewStorage, pathToNewOffsets));
            storagesCounter.incrementAndGet();
            writer = new DaoWriter(getStoragePath(storagesCounter.get()), getOffsetsPath(storagesCounter.get()));
        } finally {
            flushLock.writeLock().unlock();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (isClosed.get()) {
            return;
        }
        isClosed.set(true);
        BlockingMergeIterator.blockIterators();
        flushExecutor.shutdown();
        compactExecutor.shutdown();
        try {
            flushExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            compactExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            flushExecutor.shutdownNow();
            compactExecutor.shutdownNow();
        }
        int currentFileNumber = storagesCounter.get() - 1;
        List<Integer> numbersOfFilesToRename = new ArrayList<>();
        boolean needToRename = false;
        for (DaoReader reader : readers) {
            boolean isRemoved = reader.getIsRemoved();
            reader.close();
            if (isRemoved) {
                Files.delete(getStoragePath(currentFileNumber));
                Files.delete(getOffsetsPath(currentFileNumber--));
                needToRename = true;
            } else {
                numbersOfFilesToRename.add(currentFileNumber--);
            }
        }
        /* Если ни один из storage в памяти не был заполнен, то это означает, что никаких операций upsert или flush
         * не производилось. Если numbersOfFilesToRename тоже пуст, значит не было и компакта. Если всех вышеописанных
         * операций не производилось, то имеет смысл закончить close() здесь, чтобы не делать лишней работы
         * (все нужные ресурсы уже освобождены). */
        if (storage.isEmpty() && flushingStorage.isEmpty() && !needToRename) {
            readers.clear();
            return;
        }
        /* Так как поиск файлов при инициализации DAO зависит от количества файлов, нужно, чтобы все номера файлов
         *  в названии были меньше количества файлов, поэтому необходимо переименовать файлы. */
        Collections.reverse(numbersOfFilesToRename);
        int numberOfNewFile = 0;
        for (int numberOfFile : numbersOfFilesToRename) {
            Files.move(getStoragePath(numberOfFile), getStoragePath(numberOfNewFile), StandardCopyOption.ATOMIC_MOVE);
            Files.move(getOffsetsPath(numberOfFile), getOffsetsPath(numberOfNewFile++), StandardCopyOption.ATOMIC_MOVE);
        }
        storagesCounter.set(numberOfNewFile);
        writer = new DaoWriter(getStoragePath(storagesCounter.get()), getOffsetsPath(storagesCounter.get()));
        writer.writeDAO(storage);
        flushingStorage.clear();
        storage.clear();
        readers.clear();
    }

    private Path getStoragePath(int index) {
        return directoryPath.resolve(DATA_FILE_NAME + index + FILE_EXTENSION);
    }

    private Path getOffsetsPath(int index) {
        return directoryPath.resolve(OFFSETS_FILE_NAME + index + FILE_EXTENSION);
    }

    private Path getStoragePath(String qualifier) {
        return directoryPath.resolve(qualifier + DATA_FILE_NAME + FILE_EXTENSION);
    }

    private Path getOffsetsPath(String qualifier) {
        return directoryPath.resolve(qualifier + OFFSETS_FILE_NAME + FILE_EXTENSION);
    }

    private int validateDAOFiles() throws IOException {
        int numberOfStorages = 0;
        int numberOfOffsets = 0;
        // Удаляем файлы из директории, не относящиеся к нашей DAO, и считаем количество storage
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directoryPath)) {
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
            if (!Files.exists(getOffsetsPath(i))) {
                throw new IllegalStateException("There is no offsets file for some storage: storage number " + i);
            }
        }
        return numberOfStorages;
    }

    private List<DaoReader> initDaoReaders() throws IOException {
        List<DaoReader> resultList = new ArrayList<>();
        for (int i = storagesCounter.get() - 1; i >= 0; i--) {
            resultList.add(new DaoReader(getStoragePath(i), getOffsetsPath(i)));
        }
        return resultList;
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
}
