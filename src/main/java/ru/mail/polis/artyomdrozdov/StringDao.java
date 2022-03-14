package ru.mail.polis.artyomdrozdov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StringDao implements Dao<String, Entry<String>> {

    private final ConcurrentNavigableMap<String, Entry<String>> storage =
            new ConcurrentSkipListMap<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Config config;
    private final Path data;

    private boolean maybeHasFile = true;

    public StringDao(Config config) {
        this.config = config;
        this.data = config.basePath().resolve("data.dat");
    }

    @Override
    public Iterator<Entry<String>> get(String from, String to) {
        if (from == null) {
            from = "";
        }

        if (to == null) {
            return storage.tailMap(from).values().iterator();
        }

        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<String> get(String key) throws IOException {
        Entry<String> entry = storage.get(key);
        if (entry != null) {
            return entry;
        }
        if (!maybeHasFile) {
            return null;
        }

        lock.readLock().lock();
        try (DataInputStream inputStream = new DataInputStream(new BufferedInputStream(Files.newInputStream(
                data,
                StandardOpenOption.READ
        )))) {
            int count = inputStream.readInt();
            for (int i = 0; i < count; i++) {
                String nextKey = inputStream.readUTF();
                String nextValue = inputStream.readUTF();

                if (nextKey.equals(key)) {
                    return new BaseEntry<>(nextKey, nextValue);
                }
            }
        } catch (NoSuchFileException e) {
            maybeHasFile = false;
            return null;
        } finally {
            lock.readLock().unlock();
        }
        return null;
    }

    @Override
    public void upsert(Entry<String> entry) {
        lock.readLock().lock();
        try {
            storage.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        lock.writeLock().lock();
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                data,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        )))) {
            int size = storage.size();
            out.writeInt(size);
            for (Entry<String> value : storage.values()) {
                out.writeUTF(value.key());
                out.writeUTF(value.value());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
