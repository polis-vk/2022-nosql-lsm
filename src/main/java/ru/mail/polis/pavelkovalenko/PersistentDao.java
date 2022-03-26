package ru.mail.polis.pavelkovalenko;

import java.util.Map;
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

public class PersistentDao implements Dao<ByteBuffer, Entry<ByteBuffer>> {

    private final ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> data = new ConcurrentSkipListMap<>();
    private final NavigableMap<Integer, Entry<Path>> pathsToPairedFiles = new TreeMap<>();
    private final Config config;

    public PersistentDao(Config config) throws IOException {
        this.config = config;

        String[] files = new File(config.basePath().toString()).list();
        if (files != null && files.length > 0) {
            for (int i = 0; i < files.length / 2; ++i) {
                addPairedFiles();
            }
        }
    }

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        return new MergeIterator(from ,to, data, pathsToPairedFiles);
    }

    @Override
    public Entry<ByteBuffer> get(ByteBuffer key) throws IOException {
        Entry<ByteBuffer> result = findKeyInStorage(key);
        if (result == null) {
            result = findKeyInFile(key);
        }
        return Utils.isTombstone(result) ? null : result;
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        data.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        Entry<Path> newestFilePair = pathsToPairedFiles.lastEntry().getValue();
        Serializer.write(newestFilePair.key(), newestFilePair.value(), data);
    }

    @Override
    public void close() throws IOException {
        addPairedFiles();
        flush();
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

    private Entry<ByteBuffer> findKeyInStorage(ByteBuffer key) {
        return data.get(key);
    }

    private Entry<ByteBuffer> findKeyInFile(ByteBuffer key) throws IOException {
        Entry<ByteBuffer> result = null;
        for (Map.Entry<Integer, Entry<Path>> pathToPairedFiles: pathsToPairedFiles.entrySet()) {
            Path pathToDataFile = pathToPairedFiles.getValue().key();
            Path pathToIndexesFile = pathToPairedFiles.getValue().value();
            try (FileIterator fileIterator = new FileIterator(pathToDataFile, pathToIndexesFile, key, null)) {
                if (!fileIterator.hasNext()) {
                    continue;
                }
                result = fileIterator.next();
                if (result != null && !result.key().equals(key)) {
                    return null;
                }
            }
        }
        return result;
    }

}
