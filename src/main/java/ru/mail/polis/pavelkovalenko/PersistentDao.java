package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.iterators.MergeIterator;
import ru.mail.polis.pavelkovalenko.utils.Utils;

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

    private final ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> memorySSTable = new ConcurrentSkipListMap<>();
    private final NavigableMap<Integer /*priority*/, PairedFiles> SSTables = new TreeMap<>();
    private final Config config;
    private final Serializer serializer;

    public PersistentDao(Config config) throws IOException {
        this.config = config;

        String[] files = new File(config.basePath().toString()).list();
        if (files != null && files.length > 0) {
            for (int i = 0; i < files.length / 2; ++i) {
                addPairedFiles();
            }
        }

        this.serializer = new Serializer(SSTables);
    }

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        return new MergeIterator(from, to, serializer, memorySSTable, SSTables);
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        memorySSTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        serializer.write(SSTables.lastEntry().getValue(), memorySSTable);
    }

    @Override
    public void close() throws IOException {
        addPairedFiles();
        flush();
    }

    private void addPairedFiles() throws IOException {
        Path pathToDataFile = addFile(Utils.DATA_FILENAME);
        Path pathToIndexesFile = addFile(Utils.INDEXES_FILENAME);
        PairedFiles pathToPairedFiles = new PairedFiles(pathToDataFile, pathToIndexesFile);
        SSTables.put(SSTables.size() + 1, pathToPairedFiles);
    }

    private Path addFile(String filename) throws IOException {
        Path file = config.basePath().resolve(filename + (SSTables.size() + 1) + Utils.FILE_EXTENSION);
        createFile(file);
        return file;
    }

    private void createFile(Path filename) throws IOException {
        if (!Files.exists(filename)) {
            Files.createFile(filename);
        }
    }

}
