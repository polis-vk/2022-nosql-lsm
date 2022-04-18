package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private static final String DATA_FILE_NAME = "storage";
    private static final String OFFSETS_FILE_NAME = "offsets";
    private static final String TMP_QUALIFIER = "tmp_";
    private static final String COMPACTED_QUALIFIER = "compacted_";
    private static final String FILE_EXTENSION = ".txt";
    private final ConcurrentNavigableMap<String, BaseEntry<String>> storage = new ConcurrentSkipListMap<>();
    private final List<DaoReader> readers;
    private final Path directoryPath;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private DaoWriter writer;
    private int filesCounter;
    private volatile boolean isClosed;

    public InMemoryDao(Config config) throws IOException {
        this.directoryPath = config.basePath();
        finishCompact();
        this.filesCounter = validateDAOFiles();
        this.readers = initDaoReaders();
        this.writer = new DaoWriter(getStoragePath(filesCounter), getOffsetsPath(filesCounter));
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        if (!isClosed) {
            throw new IllegalStateException("DAO has been closed");
        }
        Queue<PriorityPeekIterator> iteratorsQueue = new PriorityQueue<>(
                Comparator.comparing((PriorityPeekIterator o) ->
                        o.peek().key()).thenComparingInt(PriorityPeekIterator::getPriorityIndex)
        );
        PriorityPeekIterator storageIterator = findCurrentStorageIteratorByRange(from, to);
        if (storageIterator.hasNext()) {
            iteratorsQueue.add(storageIterator);
        }
        lock.readLock().lock();
        try {
            for (int i = 0; i < filesCounter; i++) {
                FileIterator fileIterator = new FileIterator(from, to, readers.get(i));
                if (fileIterator.hasNext()) {
                    iteratorsQueue.add(new PriorityPeekIterator(fileIterator, i + 1));
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        return iteratorsQueue.isEmpty() ? Collections.emptyIterator() : new MergeIterator(iteratorsQueue);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        if (!isClosed) {
            throw new IllegalStateException("DAO has been closed");
        }
        BaseEntry<String> value = storage.get(key);
        if (value == null) {
            lock.readLock().lock();
            try {
                for (int i = 0; i < filesCounter; i++) {
                    value = readers.get(i).findByKey(key);
                    if (value != null) {
                        return value.value() == null ? null : value;
                    }
                }
                value = new BaseEntry<>(null, null);
            } finally {
                lock.readLock().unlock();
            }
        }
        return value.value() == null ? null : value;
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        if (!isClosed) {
            throw new IllegalStateException("DAO has been closed");
        }
        lock.readLock().lock();
        try {
            storage.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        if (!isClosed) {
            throw new IllegalStateException("DAO has been closed");
        }
        if (storage.isEmpty()) {
            return;
        }
        lock.writeLock().lock();
        try {
            writer.writeDAO(storage);
            readers.add(0, new DaoReader(getStoragePath(filesCounter), getOffsetsPath(filesCounter)));
            filesCounter++;
            writer = new DaoWriter(getStoragePath(filesCounter), getOffsetsPath(filesCounter));
            storage.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        if (!isClosed) {
            throw new IllegalStateException("DAO has been closed");
        }
        if (readers.size() <= 1 && storage.isEmpty()) {
            return;
        } else if (readers.isEmpty()) {
            flush();
            return;
        }
        flush(); // Флашим данные, чтобы не потерять при падении
        lock.writeLock().lock();
        try {
            Iterator<BaseEntry<String>> allData = all();
            int allDataSize = 0;
            while (allData.hasNext()) {
                allDataSize++;
                allData.next();
            }
            allData = all();
            Path pathToTmpDataFile = getStoragePath(TMP_QUALIFIER);
            Path pathToTmpOffsetsFile = getOffsetsPath(TMP_QUALIFIER);

            DaoWriter tmpWriter = new DaoWriter(pathToTmpDataFile, pathToTmpOffsetsFile);
            tmpWriter.writeDAOWithoutTombstones(allData, allDataSize);
            /* Если есть хотя бы один compacted файл, значит все данные были записаны.
            *  До этого не будем учитывать tmp файлы при восстановлении. */
            Files.move(pathToTmpDataFile, getStoragePath(COMPACTED_QUALIFIER), StandardCopyOption.ATOMIC_MOVE);
            Files.move(pathToTmpOffsetsFile, getOffsetsPath(COMPACTED_QUALIFIER), StandardCopyOption.ATOMIC_MOVE);

            closeReaders();
            finishCompact();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (!isClosed) {
            return;
        }
        flush();
        closeReaders();
        isClosed = true;
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
        /* Если только offsets файл compacted, то в соответствии с последовательностью на строках 185-186 значит,
        *  что мы упали между переводом файла storage и файла offsets из compacted в нормальное состояние */
        if (!isDataCompacted) {
            Files.move(pathToCompactedOffsetsFile, getOffsetsPath(0), StandardCopyOption.ATOMIC_MOVE);
            return;
        }
        Path pathToTmpOffsetsFile = getOffsetsPath(TMP_QUALIFIER);
        /* Если data файл compacted и offsets файл не compacted, значит, что не успели перевести файл offsets из tmp в
        *  compacted. При этом запись полностью прошла в соответствии с последовательностью на строках 144-145 */
        if (Files.exists(pathToTmpOffsetsFile)) {
            Files.move(pathToTmpOffsetsFile, pathToCompactedOffsetsFile, StandardCopyOption.ATOMIC_MOVE);
        }
        /* Код на строках 184-187 выполниться и в том случае, если мы зайдём в данный метод из метода compact, поскольку
        *  в таком случае у нас оба файла будут compacted. */
        removeOldFiles();
        Files.move(pathToCompactedDataFile, getStoragePath(0), StandardCopyOption.ATOMIC_MOVE);
        Files.move(pathToCompactedOffsetsFile, getOffsetsPath(0), StandardCopyOption.ATOMIC_MOVE);
    }

    private void removeOldFiles() throws IOException {
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directoryPath)) {
            for (Path file : directoryStream) {
                String fileName = file.getFileName().toString();
                if (fileName.startsWith(DATA_FILE_NAME) || fileName.startsWith(OFFSETS_FILE_NAME)) {
                    Files.delete(file);
                }
            }
        }
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
                throw new IllegalStateException("There is no offsets file for storage");
            }
        }
        return numberOfStorages;
    }

    private List<DaoReader> initDaoReaders() throws IOException {
        List<DaoReader> resultList = new ArrayList<>();
        for (int i = filesCounter - 1; i >= 0; i--) {
            resultList.add(new DaoReader(getStoragePath(i), getOffsetsPath(i)));
        }
        return resultList;
    }

    private PriorityPeekIterator findCurrentStorageIteratorByRange(String from, String to) {
        if (from == null && to == null) {
            return new PriorityPeekIterator(storage.values().iterator(), 0);
        } else if (from == null) {
            return new PriorityPeekIterator(storage.headMap(to).values().iterator(), 0);
        } else if (to == null) {
            return new PriorityPeekIterator(storage.tailMap(from).values().iterator(), 0);
        } else {
            return new PriorityPeekIterator(storage.subMap(from, to).values().iterator(), 0);
        }
    }

    private void closeReaders() throws IOException {
        for (DaoReader reader : readers) {
            reader.close();
        }
    }
}
