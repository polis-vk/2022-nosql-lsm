package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<ByteBuffer, Entry<ByteBuffer>> {

    private final ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> data = new ConcurrentSkipListMap<>();
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    private static final NavigableMap<Integer, Entry<Path>> pathsToPairedFiles = new TreeMap<>();
    private final Reader reader;
    private final Writer writer;
    private final Config config;

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        addPairedFiles();

        reader = new Reader(data, pathsToPairedFiles);
        writer = new Writer(data, pathsToPairedFiles);
    }

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        return reader.get(from, to);
    }

    @Override
    public Entry<ByteBuffer> get(ByteBuffer key) throws IOException {
        try {
            rwlock.readLock().lock();
            return reader.get(key);
        } finally {
            rwlock.readLock().unlock();
        }
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        try {
            rwlock.readLock().lock();
            data.put(entry.key(), entry);
        } finally {
            rwlock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            rwlock.writeLock().lock();
            writer.write();
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        addPairedFiles();
    }

    private void addPairedFiles() throws IOException {
        Path pathToDataFile = addDataFile();
        Path pathToIndexesFile = addIndexFile();
        BaseEntry<Path> pathToPairedFiles = new BaseEntry<>(pathToDataFile, pathToIndexesFile);
        pathsToPairedFiles.put(pathsToPairedFiles.size(), pathToPairedFiles);
    }

    private Path addDataFile() throws IOException {
        Path newDataFile = config.basePath().resolve("data" + pathsToPairedFiles.size() + ".txt");
        Files.createFile(newDataFile);
        return newDataFile;
    }

    private Path addIndexFile() throws IOException {
        Path newIndexesFile = config.basePath().resolve("indexes" + pathsToPairedFiles.size() + ".txt");
        Files.createFile(newIndexesFile);
        return newIndexesFile;
    }

}
