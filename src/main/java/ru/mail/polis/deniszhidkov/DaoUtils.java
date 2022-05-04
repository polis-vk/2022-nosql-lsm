package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class DaoUtils {

    public static final String DATA_FILE_NAME = "storage";
    public static final String OFFSETS_FILE_NAME = "offsets";
    private static final String COMPACTED_QUALIFIER_NAME = "compacted_";
    private static final String FILE_EXTENSION = ".txt";
    private static final String TMP_QUALIFIER_NAME = "tmp_";
    private static final int COMPACTED_QUALIFIER = -1;
    private static final int TMP_QUALIFIER = -2;
    private final Path basePath;

    public DaoUtils(Path basePath) {
        this.basePath = basePath;
    }

    public Path resolvePath(String typeOfFile, int qualifier) {
        return switch (qualifier) {
            case COMPACTED_QUALIFIER -> basePath.resolve(COMPACTED_QUALIFIER_NAME + typeOfFile + FILE_EXTENSION);
            case TMP_QUALIFIER -> basePath.resolve(TMP_QUALIFIER_NAME + typeOfFile + FILE_EXTENSION);
            default -> basePath.resolve(typeOfFile + qualifier + FILE_EXTENSION);
        };
    }

    public static long getEntrySize(BaseEntry<String> entry) {
        return entry.value() == null
                ? entry.key().length() * 2L
                : (entry.key().length() + entry.value().length()) * 2L;
    }

    public void validateDAOFiles() throws IOException {
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
            if (!Files.exists(resolvePath(OFFSETS_FILE_NAME, i))) {
                throw new IllegalStateException("There is no offsets file for some storage: storage number " + i);
            }
        }
    }

    public CopyOnWriteArrayList<DaoReader> initDaoReaders() throws IOException {
        CopyOnWriteArrayList<DaoReader> resultList = new CopyOnWriteArrayList<>();
        // Методом validateDaoFiles() гарантируется, что существуют все файлы по порядку от 0 до N.
        for (int i = 0; ; i++) {
            try {
                resultList.add(new DaoReader(
                                resolvePath(DATA_FILE_NAME, i),
                                resolvePath(OFFSETS_FILE_NAME, i)
                        )
                );
            } catch (FileNotFoundException e) {
                break;
            }
        }
        Collections.reverse(resultList);
        return resultList;
    }

    public void closeReaders(CopyOnWriteArrayList<DaoReader> readers) throws IOException {
        for (DaoReader reader : readers) {
            reader.close();
        }
    }

    public void removeOldFiles() throws IOException {
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(basePath)) {
            for (Path file : directoryStream) {
                String fileName = file.getFileName().toString();
                if (fileName.startsWith(DATA_FILE_NAME) || fileName.startsWith(OFFSETS_FILE_NAME)) {
                    Files.delete(file);
                }
            }
        }
    }

    public void addAllIterators(Queue<PriorityPeekIterator> iteratorsQueue,
                                ConcurrentNavigableMap<String, BaseEntry<String>> inMemory,
                                ConcurrentNavigableMap<String, BaseEntry<String>> inFlushing,
                                CopyOnWriteArrayList<DaoReader> readers,
                                String from,
                                String to
    ) throws IOException {
        int priorityIndex = 0;
        priorityIndex = addInMemoryIteratorByRange(iteratorsQueue, inMemory, from, to, priorityIndex);
        priorityIndex = addInMemoryIteratorByRange(iteratorsQueue, inFlushing, from, to, priorityIndex);
        addInStorageIteratorsByRange(iteratorsQueue, readers, from, to, priorityIndex);
    }

    private int addInMemoryIteratorByRange(Queue<PriorityPeekIterator> iteratorsQueue,
                                           ConcurrentNavigableMap<String, BaseEntry<String>> storage,
                                           String from,
                                           String to,
                                           int index
    ) {
        int priorityIndex = index;
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

    public void addInStorageIteratorsByRange(Queue<PriorityPeekIterator> iteratorsQueue,
                                             CopyOnWriteArrayList<DaoReader> readers,
                                             String from,
                                             String to,
                                             int index
    ) throws IOException {
        int priorityIndex = index;
        for (DaoReader reader : readers) {
            FileIterator fileIterator = new FileIterator(from, to, reader);
            if (fileIterator.hasNext()) {
                iteratorsQueue.add(new PriorityPeekIterator(fileIterator, priorityIndex++));
            }
        }
    }

    public BaseEntry<String> findInMemoryByKey(String key,
                                               ConcurrentNavigableMap<String, BaseEntry<String>> inMemory,
                                               ConcurrentNavigableMap<String, BaseEntry<String>> inFlushing) {
        BaseEntry<String> value = inMemory.get(key);
        if (value != null) {
            return value;
        }
        value = inFlushing.get(key);
        return value;
    }

    public BaseEntry<String> findInStorageByKey(String key,
                                                CopyOnWriteArrayList<DaoReader> readers
    ) throws IOException {
        BaseEntry<String> value;
        for (DaoReader reader : readers) {
            value = reader.findByKey(key);
            if (value != null) {
                return value.value() == null ? null : value;
            }
        }
        return null;
    }
}
