package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import java.io.File;
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
    private final NavigableMap<Integer, Entry<Path>> pathsToPairedFiles = new TreeMap<>();
    private final Reader reader;
    private final Writer writer;
    private final Config config;

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        String[] files = new File(config.basePath().toString()).list();
        if (files == null || files.length == 0) {
            addPairedFiles();
        } else {
            for (int i = 0; i < files.length / 2; ++i) {
                addPairedFiles();
            }
        }

        reader = new Reader(data, pathsToPairedFiles);
        writer = new Writer(data, pathsToPairedFiles);
    }

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        return reader.get(from, to);
    }

    @Override
    public Entry<ByteBuffer> get(ByteBuffer key) throws IOException {
        return reader.get(key);
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        data.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        writer.write();
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
        pathsToPairedFiles.put(pathsToPairedFiles.size() + 1, pathToPairedFiles);
    }

    private Path addDataFile() throws IOException {
        Path newDataFile = config.basePath()
                .resolve(Utils.DATA_FILENAME + (pathsToPairedFiles.size() + 1) + Utils.FILE_EXTENSION);
        createFile(newDataFile);
        return newDataFile;
    }

    private Path addIndexFile() throws IOException {
        Path newIndexesFile = config.basePath()
                .resolve(Utils.INDEXES_FILENAME + (pathsToPairedFiles.size() + 1) + Utils.FILE_EXTENSION);
        createFile(newIndexesFile);
        return newIndexesFile;
    }

    private void createFile(Path filename) throws IOException {
        if (!Files.exists(filename)) {
            Files.createFile(filename);
        }
    }

}
