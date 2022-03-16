package ru.mail.polis.sasharudnev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private final ConcurrentNavigableMap<String, BaseEntry<String>> data = new ConcurrentSkipListMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean fileIsNotExists = true;

    private final ReaderInDao reader;
    private final WriterInDao writer;

    public InMemoryDao(Config config) throws IOException {
        Path path = config.basePath().resolve("data.dat");
        this.reader = new ReaderInDao(path);
        this.writer = new WriterInDao(path);
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) {
        Map<String, BaseEntry<String>> dataSet;
        boolean isFromEqualsNull = from == null;
        boolean isToEqualsNull = to == null;

        if (isFromEqualsNull && isToEqualsNull) {
            dataSet = data;
        } else if (isFromEqualsNull) {
            dataSet = data.headMap(to);
        } else if (isToEqualsNull) {
            dataSet = data.tailMap(from);
        } else {
            dataSet = data.subMap(from, to);
        }

        return dataSet.values().iterator();
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        BaseEntry<String> entry = data.get(key);
        if (entry != null) {
            return entry;
        }
        if (fileIsNotExists) {
            return null;
        }
        lock.readLock().lock();
        try {
            BaseEntry<String> readEntry = reader.readInDao(key);
            if (readEntry != null) {
                return readEntry;
            }
        } catch (NoSuchFileException de) {
            fileIsNotExists = false;
            return null;
        } finally {
            lock.readLock().unlock();
        }
        return null;
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        lock.readLock().lock();
        try {
            data.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        lock.writeLock().lock();
        try {
            writer.writeInDao(data);
            data.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
