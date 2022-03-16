package ru.mail.polis.arturgaleev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.test.arturgaleev.FileDBReader;
import ru.mail.polis.test.arturgaleev.FileDBWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> dataBase = new ConcurrentSkipListMap<>();
    private final Config config;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        if (!Files.isDirectory(config.basePath())) {
            Files.createDirectories(config.basePath());
        }
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        lock.readLock().lock();
        try {
            if (from == null && to == null) {
                return dataBase.values().iterator();
            }
            if (from != null && to == null) {
                return dataBase.tailMap(from).values().iterator();
            }
            if (from == null) {
                return dataBase.headMap(to).values().iterator();
            }
            return dataBase.subMap(from, to).values().iterator();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        lock.readLock().lock();
        try {
            BaseEntry<ByteBuffer> entry = dataBase.get(key);
            if (entry != null) {
                return entry;
            }

            BaseEntry<ByteBuffer> value = dataBase.get(key);
            if (value != null) {
                return value;
            }
            if (!Files.exists(config.basePath().resolve("1.txt"))) {
                return null;
            }
            try (FileDBReader reader = new FileDBReader(config.basePath().resolve("1.txt"))) {
                return reader.getByKey(key);
            }
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
            final int[] sz = {Integer.BYTES + dataBase.size() * Integer.BYTES};
            dataBase.forEach((key, val) -> sz[0] += key.limit() + val.value().limit() + 2 * Integer.BYTES);
            try (FileDBWriter writer = new FileDBWriter(config.basePath().resolve("1.txt"), sz[0])) {
                writer.writeMap(dataBase);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
