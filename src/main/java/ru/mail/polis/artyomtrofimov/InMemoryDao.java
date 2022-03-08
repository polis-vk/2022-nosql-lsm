package ru.mail.polis.artyomtrofimov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, Entry<String>> {
    private static final int MAX_CAPACITY = 20_000;
    private static final String FILENAME = "db.txt";
    private final ConcurrentNavigableMap<String, Entry<String>> data = new ConcurrentSkipListMap<>();
    private final Config config;
    private long lastWritePos;
    private volatile boolean commit;

    public InMemoryDao(Config config) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException("Config shouldn't be null");
        }
        this.config = config;
        loadFromFile();
    }

    private long loadFromFile() throws IOException {
        try (RandomAccessFile input = new RandomAccessFile(getPath().toString(), "r")) {
            String line;
            while (data.size() <= MAX_CAPACITY) {
                line = input.readUTF();
                int delimiterIndex = line.indexOf(' ');
                if (delimiterIndex == -1) {
                    continue;
                }
                Entry<String> entry = new BaseEntry<>(line.substring(0, delimiterIndex),
                        line.substring(delimiterIndex + 1));
                data.put(entry.key(), entry);
            }
            return input.getFilePointer();
        } catch (EOFException e) {
            return -1;
        }
    }

    private Path getPath() throws IOException {
        Path path = config.basePath();
        if (Files.notExists(path)) {
            Files.createDirectories(path);
        }
        path = path.resolve(FILENAME);
        if (Files.notExists(path)) {
            Files.createFile(path);
        }
        return path;
    }

    @Override
    public Iterator<Entry<String>> get(String from, String to) throws IOException {
        boolean isFromNull = from == null;
        boolean isToNull = to == null;
        if (isFromNull && isToNull) {
            return data.values().iterator();
        }
        if (isFromNull) {
            return data.headMap(to).values().iterator();
        }
        if (isToNull) {
            return data.tailMap(from).values().iterator();
        }
        return data.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<String> get(String key) throws IOException {
        if (!data.containsKey(key)) {
            Entry<String> entry = findInFileByKey(key);
            if (entry != null) {
                data.put(entry.key(), entry);
            }
            return entry;
        }
        return data.get(key);
    }

    private Entry<String> findInFileByKey(String key) throws IOException {
        try (RandomAccessFile input = new RandomAccessFile(getPath().toString(), "r")) {
            input.seek(0);
            String line;
            while (input.getFilePointer() <= input.length()) {
                line = input.readUTF();
                int delimiterIndex = line.indexOf(' ');
                if (delimiterIndex == -1) {
                    continue;
                }
                String currentKey = line.substring(0, delimiterIndex);
                if (key.equals(currentKey)) {
                    return new BaseEntry<>(currentKey, line.substring(delimiterIndex + 1));
                }
            }
            return null;
        } catch (EOFException e) {
            return null;
        }
    }

    @Override
    public void upsert(Entry<String> entry) {
        data.put(entry.key(), entry);
        commit = false;
    }

    @Override
    public void flush() throws IOException {
        if (commit) {
            return;
        }
        try (RandomAccessFile output = new RandomAccessFile(getPath().toString(), "rw")) {
            output.seek(lastWritePos);
            StringBuilder result = new StringBuilder();
            for (Entry<String> value : data.values()) {
                result.append(value.key()).append(' ').append(value.value());
                output.writeUTF(result.toString());
                result.setLength(0);
            }
            lastWritePos = output.getFilePointer();
        }
        commit = true;
    }
}
