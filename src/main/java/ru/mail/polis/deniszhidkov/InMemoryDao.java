package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
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
    private static final String FILE_EXTENSION = ".txt";
    private final ConcurrentNavigableMap<String, BaseEntry<String>> storage = new ConcurrentSkipListMap<>();
    private final DaoWriter writer;
    private final List<DaoReader> readers;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final int filesCounter;

    public InMemoryDao(Config config) throws IOException {
        File[] filesInDirectory = new File(String.valueOf(config.basePath())).listFiles();
        this.filesCounter = filesInDirectory == null ? 0 : filesInDirectory.length / 2;
        this.readers = initDaoReaders(config);
        this.writer = new DaoWriter(
                config.basePath().resolve(DATA_FILE_NAME + filesCounter + FILE_EXTENSION),
                config.basePath().resolve(OFFSETS_FILE_NAME + filesCounter + FILE_EXTENSION)
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
    public void flush() throws IOException {
        lock.writeLock().lock();
        try {
            writer.writeDAO(storage);
            storage.clear();
            for (DaoReader reader : readers) {
                reader.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
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

    private List<DaoReader> initDaoReaders(Config config) throws IOException {
        List<DaoReader> resultList = new ArrayList<>();
        for (int i = filesCounter - 1; i >= 0; i--) {
            long[] fileOffsets;
            try (DataInputStream offsetsFileReader = new DataInputStream(
                    new BufferedInputStream(
                            Files.newInputStream(
                                    config.basePath().resolve(OFFSETS_FILE_NAME + i + FILE_EXTENSION),
                                    StandardOpenOption.READ
                            )))) {
                fileOffsets = new long[offsetsFileReader.readInt()];
                for (int j = 0; j < fileOffsets.length; j++) {
                    fileOffsets[j] = offsetsFileReader.readLong();
                }
            } catch (FileNotFoundException e) {
                throw new FileNotFoundException("File " + OFFSETS_FILE_NAME + i + FILE_EXTENSION + "doesn't exists");
            }
            try {
                resultList.add(new DaoReader(
                                new RandomAccessFile(
                                        config.basePath().resolve(DATA_FILE_NAME + i + FILE_EXTENSION).toString(),
                                        "r"),
                                fileOffsets
                        )
                );
            } catch (FileNotFoundException e) {
                throw new FileNotFoundException("File " + DATA_FILE_NAME + i + FILE_EXTENSION + "doesn't exists");
            }
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
}
