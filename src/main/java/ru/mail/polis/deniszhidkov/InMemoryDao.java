package ru.mail.polis.deniszhidkov;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private static final String FILE_NAME = "storage.txt";
    private boolean hasFile = true;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final ConcurrentNavigableMap<String, BaseEntry<String>> storage = new ConcurrentSkipListMap<>();
    private final DaoReader reader;
    private final DaoWriter writer;

    public InMemoryDao(Config config) throws IOException {
        Path pathToFile = config.basePath().resolve(FILE_NAME);
        this.reader = new DaoReader(pathToFile);
        this.writer = new DaoWriter(pathToFile);
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        Collection<BaseEntry<String>> values;
        if (from == null && to == null) {
            values = storage.values();
        } else if (from == null) {
            values = storage.headMap(to).values();
        } else if (to == null) {
            values = storage.tailMap(from).values();
        } else {
            values = storage.subMap(from, to).values();
        }
        return values.iterator();
    }

    @Override
    public BaseEntry<String> get(@NotNull String key) throws IOException {
        if (!hasFile) {
            return null;
        }
        BaseEntry<String> value = storage.get(key);
        lock.readLock().lock();
        try {
            if (value == null) {
                try {
                    value = reader.findEntryByKey(key);
                } catch (NoSuchFileException e) {
                    hasFile = false;
                    return null;
                }
            }
        } finally {
            lock.readLock().unlock();
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
    public void upsert(@NotNull BaseEntry<String> entry) {
        lock.readLock().lock();
        try {
            storage.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }
}
