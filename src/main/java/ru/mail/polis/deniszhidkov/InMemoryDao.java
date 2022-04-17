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
    private static final String TMP_FILE_NAME = "tmp";
    private static final String COMPACTED_QUALIFIER = "_compacted_";
    private static final String FILE_EXTENSION = ".txt";
    private final ConcurrentNavigableMap<String, BaseEntry<String>> storage = new ConcurrentSkipListMap<>();
    private final List<DaoReader> readers;
    private final Path directoryPath;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private DaoWriter writer;
    private int filesCounter;

    public InMemoryDao(Config config) throws IOException {
        this.directoryPath = config.basePath();
        finishCompactIfNecessary();
        this.filesCounter = validateDAOFiles();
        this.readers = initDaoReaders();
        this.writer = new DaoWriter(
                directoryPath.resolve(DATA_FILE_NAME + filesCounter + FILE_EXTENSION),
                directoryPath.resolve(OFFSETS_FILE_NAME + filesCounter + FILE_EXTENSION)
        );
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
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
        lock.readLock().lock();
        try {
            storage.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        if (storage.isEmpty()) {
            return;
        }
        lock.writeLock().lock();
        try {
            writer.writeDAO(storage);
            filesCounter++;
            writer = new DaoWriter(
                    directoryPath.resolve(DATA_FILE_NAME + filesCounter + FILE_EXTENSION),
                    directoryPath.resolve(OFFSETS_FILE_NAME + filesCounter + FILE_EXTENSION)
            );
            storage.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        if (readers.size() <= 1 && storage.isEmpty()) {
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
            Path pathToTmpDataFile = directoryPath.resolve(TMP_FILE_NAME + DATA_FILE_NAME + FILE_EXTENSION);
            Path pathToTmpOffsetsFile = directoryPath.resolve(TMP_FILE_NAME + OFFSETS_FILE_NAME + FILE_EXTENSION);
            DaoWriter tmpWriter = new DaoWriter(
                    pathToTmpDataFile,
                    pathToTmpOffsetsFile
            );
            tmpWriter.writeDAOWithoutTombstones(allData, allDataSize);
            Files.move(
                    pathToTmpDataFile,
                    directoryPath.resolve(COMPACTED_QUALIFIER + DATA_FILE_NAME + FILE_EXTENSION),
                    StandardCopyOption.ATOMIC_MOVE
            );
            Files.move(
                    pathToTmpOffsetsFile,
                    directoryPath.resolve(COMPACTED_QUALIFIER + OFFSETS_FILE_NAME + FILE_EXTENSION),
                    StandardCopyOption.ATOMIC_MOVE
            );
            closeReaders();
            removeOldFiles();
            Files.move(
                    directoryPath.resolve(COMPACTED_QUALIFIER + DATA_FILE_NAME + FILE_EXTENSION),
                    directoryPath.resolve(DATA_FILE_NAME + 0 + FILE_EXTENSION),
                    StandardCopyOption.ATOMIC_MOVE
            );
            Files.move(
                    directoryPath.resolve(COMPACTED_QUALIFIER + OFFSETS_FILE_NAME + FILE_EXTENSION),
                    directoryPath.resolve(OFFSETS_FILE_NAME + 0 + FILE_EXTENSION),
                    StandardCopyOption.ATOMIC_MOVE
            );
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        closeReaders();
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
            if (!Files.exists(directoryPath.resolve(OFFSETS_FILE_NAME + i + FILE_EXTENSION))) {
                throw new IllegalStateException("There is no offsets file for storage");
            }
        }
        return numberOfStorages;
    }

    private List<DaoReader> initDaoReaders() throws IOException {
        List<DaoReader> resultList = new ArrayList<>();
        for (int i = filesCounter - 1; i >= 0; i--) {
            resultList.add(new DaoReader(
                    directoryPath.resolve(DATA_FILE_NAME + i + FILE_EXTENSION),
                    directoryPath.resolve(OFFSETS_FILE_NAME + i + FILE_EXTENSION)
            ));
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

    private void finishCompactIfNecessary() throws IOException {

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


    private void closeReaders() throws IOException {
        for (DaoReader reader : readers) {
            reader.close();
        }
    }
}
