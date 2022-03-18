package ru.mail.polis.alexanderkiselyov;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
    private final Config config;
    private final int bufferSize = 1000 * Character.BYTES;
    private static final String FILE_NAME = "myData";
    private long filesCount;

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        pairs = new ConcurrentSkipListMap<>(Arrays::compare);
        if (Files.exists(config.basePath())) {
            try (Stream<Path> stream = Files.list(config.basePath())) {
                filesCount = stream.count();
            }
        } else {
            filesCount = 0;
        }
    }

    @Override
    public Iterator<BaseEntry<byte[]>> get(byte[] from, byte[] to) {
        if (from == null && to == null) {
            return pairs.values().iterator();
        } else if (from == null) {
            return pairs.headMap(to).values().iterator();
        } else if (to == null) {
            return pairs.tailMap(from).values().iterator();
        }
        return pairs.subMap(from, to).values().iterator();
    }

    @Override
    public BaseEntry<byte[]> get(byte @NotNull [] key) throws IOException {
        BaseEntry<byte[]> value = pairs.get(key);
        if (value != null && Arrays.equals(value.key(), key)) {
            return value;
        }
        return findInFiles(key);
    }

    @Override
    public void upsert(BaseEntry<byte[]> entry) {
        pairs.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        Path newFilePath = config.basePath().resolve(FILE_NAME + filesCount + ".txt");
        if (!Files.exists(newFilePath)) {
            Files.createFile(newFilePath);
        }
        try (FileOutputStream fos = new FileOutputStream(String.valueOf(newFilePath));
             BufferedOutputStream writer = new BufferedOutputStream(fos, bufferSize)) {
            for (var pair : pairs.entrySet()) {
                writer.write(pair.getKey().length);
                writer.write(pair.getKey());
                writer.write(pair.getValue().value().length);
                writer.write(pair.getValue().value());
            }
        }
        filesCount++;
    }

    private BaseEntry<byte[]> findInFiles(byte[] key) throws IOException {
        for (long i = filesCount - 1; i >= 0; i--) {
            Path currentFile = config.basePath().resolve(FILE_NAME + i + ".txt");
            try (FileInputStream fis = new FileInputStream(String.valueOf(currentFile));
                 BufferedInputStream reader = new BufferedInputStream(fis, bufferSize)) {
                while (reader.available() != 0) {
                    int keyLength = reader.read();
                    byte[] currentKey = reader.readNBytes(keyLength);
                    int valueLength = reader.read();
                    byte[] currentValue = reader.readNBytes(valueLength);
                    if (Arrays.equals(currentKey, key)) {
                        reader.close();
                        fis.close();
                        return new BaseEntry<>(currentKey, currentValue);
                    }
                }
            }
        }
        return null;
    }
}
