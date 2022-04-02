package ru.mail.polis.arturgaleev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.test.arturgaleev.DBReader;
import ru.mail.polis.test.arturgaleev.FileDBWriter;
import ru.mail.polis.test.arturgaleev.MergeIterator;
import ru.mail.polis.test.arturgaleev.PriorityPeekingIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> dataBase = new ConcurrentSkipListMap<>();
    private final Config config;
    private final DBReader reader;

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        if (!Files.isDirectory(config.basePath())) {
            Files.createDirectories(config.basePath());
        }
        reader = new DBReader(config.basePath());
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        lock.readLock().lock();
        try {
            Iterator<BaseEntry<ByteBuffer>> dataBaseIterator;
            if (from == null && to == null) {
                dataBaseIterator = dataBase.values().iterator();
            } else if (from != null && to == null) {
                dataBaseIterator = dataBase.tailMap(from).values().iterator();
            } else if (from == null) {
                dataBaseIterator = dataBase.headMap(to).values().iterator();
            } else {
                dataBaseIterator = dataBase.subMap(from, to).values().iterator();
            }
            return new MergeIterator(
                    new PriorityPeekingIterator<>(0, reader.get(from, to)),
                    new PriorityPeekingIterator<>(1, dataBaseIterator)
            );
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) {
        lock.readLock().lock();
        try {
            BaseEntry<ByteBuffer> entry = dataBase.get(key);
            if (entry != null) {
                return entry.value() == null ? null : entry;
            }
            return reader.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        lock.readLock().lock();
        try {
            dataBase.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        lock.writeLock().lock();
        try {
            try (FileDBWriter writer =
                         new FileDBWriter(config.basePath().resolve(reader.getNumberOfFiles() + ".txt"))) {
                writer.writeMap(dataBase);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
