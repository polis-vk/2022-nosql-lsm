package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {
    private final NavigableMap<byte[], BaseEntry<byte[]>> pairs;
    private static final String FILE_NAME = "myData";
    private long filesCount;
    private final FileOperations fileOperations;

    public InMemoryDao(Config config) throws IOException {
        pairs = new ConcurrentSkipListMap<>(Arrays::compare);
        if (Files.exists(config.basePath())) {
            try (Stream<Path> stream = Files.list(config.basePath())) {
                filesCount = stream.filter(f -> String.valueOf(f.getFileName()).contains(FILE_NAME)).count();
            }
        } else {
            filesCount = 0;
        }
        fileOperations = new FileOperations(config, filesCount, FILE_NAME);
    }

    @Override
    public Iterator<BaseEntry<byte[]>> get(byte[] from, byte[] to) throws IOException {
        Iterator<BaseEntry<byte[]>> memoryIterator;
        if (from == null && to == null) {
            memoryIterator = pairs.values().iterator();
        } else if (from == null) {
            memoryIterator = pairs.headMap(to).values().iterator();
        } else if (to == null) {
            memoryIterator = pairs.tailMap(from).values().iterator();
        } else {
            memoryIterator = pairs.subMap(from, to).values().iterator();
        }
        Iterator<BaseEntry<byte[]>> diskIterator = fileOperations.diskIterator(from, to);
        return new MergeIterator(memoryIterator, diskIterator);
    }

    @Override
    public BaseEntry<byte[]> get(byte[] key) throws IOException {
        BaseEntry<byte[]> value = pairs.get(key);
        if (value != null && value.value() == null) {
            return null;
        }
        if (value != null && Arrays.equals(value.key(), key)) {
            return value;
        }
        BaseEntry<byte[]> fileEntry = fileOperations.findInFiles(key);
        if (fileEntry == null || fileEntry.value() == null) {
            return null;
        }
        return fileEntry;
    }

    @Override
    public void upsert(BaseEntry<byte[]> entry) {
        pairs.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        fileOperations.saveData(pairs);
        fileOperations.saveIndexes(pairs);
        filesCount++;
        pairs.clear();
    }
}
