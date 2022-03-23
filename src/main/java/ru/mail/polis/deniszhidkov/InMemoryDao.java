package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private static final String DATA_FILE_NAME = "storage";
    private static final String OFFSETS_FILE_NAME = "offsets";
    private static final String FILE_EXTENSION = ".txt";
    private final Config config;
    private final DaoWriter writer;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ConcurrentNavigableMap<String, BaseEntry<String>> storage = new ConcurrentSkipListMap<>();
    private final int filesCounter;

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        File[] filesInDirectory = new File(String.valueOf(config.basePath())).listFiles();
        this.filesCounter = filesInDirectory == null ? 0 : filesInDirectory.length / 2;
        Path[] paths = resolvePaths(filesCounter);
        this.writer = new DaoWriter(paths[0], paths[1]);
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        Queue<PeekIterator> queueOfIterators = new ArrayDeque<>();
        PeekIterator storageIterator;
        if (from == null && to == null) {
            storageIterator = new PeekIterator(storage.values().iterator());
        } else if (from == null) {
            storageIterator = new PeekIterator(storage.headMap(to).values().iterator());
        } else if (to == null) {
            storageIterator = new PeekIterator(storage.tailMap(from).values().iterator());
        } else {
            storageIterator = new PeekIterator(storage.subMap(from, to).values().iterator());
        }
        if (storageIterator.hasNext()) {
            queueOfIterators.add(storageIterator);
        }
        lock.readLock().lock();
        try {
            for (int i = filesCounter - 1; i >= 0; i--) {
                Path[] paths = resolvePaths(i);
                FileIterator fileIterator = new FileIterator(from, to, paths[0], paths[1]);
                if (fileIterator.hasNext()) {
                    queueOfIterators.add(new PeekIterator(fileIterator));
                }
            }
        } catch (NoSuchFileException ignore) {
            // Если нет какого-то одного файла, не факт, что нет и других
        } finally {
            lock.readLock().unlock();
        }
        return queueOfIterators.isEmpty() ? Collections.emptyIterator() : new MergeIterator(queueOfIterators);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        BaseEntry<String> value = storage.get(key);
        if (value == null) {
            lock.readLock().lock();
            try {
                for (int i = filesCounter - 1; i >= 0; i--) {
                    Path[] paths = resolvePaths(i);
                    DaoReader reader = new DaoReader(paths[0], paths[1]);
                    value = reader.findByKey(key);
                    if (value != null) {
                        return value;
                    }
                }
            } finally {
                lock.readLock().unlock();
            }
        }
        return value;
    }

    @Override
    public void flush() throws IOException {
        lock.writeLock().lock();
        try {
            writer.writeDAO(storage);
            storage.clear();
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

    private Path[] resolvePaths(int numberOfFile) {
        return new Path[]{
                config.basePath().resolve(DATA_FILE_NAME + numberOfFile + FILE_EXTENSION),
                config.basePath().resolve(OFFSETS_FILE_NAME + numberOfFile + FILE_EXTENSION)
        };
    }
}
