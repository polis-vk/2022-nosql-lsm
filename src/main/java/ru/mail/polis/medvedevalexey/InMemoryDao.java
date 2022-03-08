package ru.mail.polis.medvedevalexey;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;

public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {

    private static final Logger LOGGER = Logger.getLogger(InMemoryDao.class.getName());

    private static final String KEY_VALUE_SEPARATOR = " ";
    private static final String LINE_SEPARATOR = "\n";
    private static final int FLUSH_THRESHOLD = 100_000;
    private static final String SUFFIX = ".dat";

    private final Path path;
    private int generation;

    private final ConcurrentNavigableMap<byte[], BaseEntry<byte[]>> storage =
            new ConcurrentSkipListMap<>(Arrays::compare);

    public InMemoryDao() {
        this.path = null;
    }

    public InMemoryDao(Config config) {
        this.path = config.basePath();
        File[] files = path.toFile().listFiles();
        generation = files == null ? 0 : files.length;
    }

    @Override
    public Iterator<BaseEntry<byte[]>> get(byte[] from, byte[] to) {
        if (from == null && to == null) {
            return storage.values().iterator();
        }
        if (from == null) {
            return storage.headMap(to).values().iterator();
        }
        if (to == null) {
            return storage.tailMap(from).values().iterator();
        }
        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public BaseEntry<byte[]> get(byte[] key) throws IOException {
        BaseEntry<byte[]> entry = storage.get(key);
        return entry == null ? getFromFile(key) : entry;
    }

    @Override
    public void upsert(BaseEntry<byte[]> entry) {
        if (entry == null) {
            throw new IllegalArgumentException();
        }
        storage.put(entry.key(), entry);

        if (storage.size() > FLUSH_THRESHOLD) {
            try {
                flush();
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, e.toString());
            }
        }
    }

    @Override
    public void flush() throws IOException {
        Path newFilePath = this.path.resolve(generation + SUFFIX);

        if (!Files.exists(newFilePath)) {
            Files.createFile(newFilePath);
        }

        StringBuilder builder = new StringBuilder();
        try (BufferedWriter writer = Files.newBufferedWriter(newFilePath)) {
            for (BaseEntry<byte[]> entry : storage.values()) {
                builder.setLength(0);
                builder.append(new String(entry.key(), UTF_8))
                        .append(KEY_VALUE_SEPARATOR)
                        .append(new String(entry.value(), UTF_8))
                        .append(LINE_SEPARATOR);
                writer.write(builder.toString());
            }
            generation++;
        }

        storage.clear();
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private BaseEntry<byte[]> getFromFile(byte[] requiredKey) throws IOException {
        Path filePath;
        String line;
        String[] lineParts;
        byte[] key;
        byte[] value;

        for (int i = generation - 1; i >= 0; i--) {
            filePath = this.path.resolve(i + SUFFIX);

            try (BufferedReader reader = Files.newBufferedReader(filePath)) {
                while ((line = reader.readLine()) != null) {
                    lineParts = line.split(KEY_VALUE_SEPARATOR);
                    key = lineParts[0].getBytes(UTF_8);
                    if (Arrays.compare(requiredKey, key) == 0) {
                        value = lineParts[1].getBytes(UTF_8);
                        return new BaseEntry<>(requiredKey, value);
                    }
                }
            }
        }

        return null;
    }
}
