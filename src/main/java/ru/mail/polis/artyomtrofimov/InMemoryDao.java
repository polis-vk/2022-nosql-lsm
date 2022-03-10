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
    }

    private Path getPath() throws IOException {
        Path path = config.basePath();
        Files.createDirectories(path);
        path = path.resolve(FILENAME);
        path.toFile().createNewFile();
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
        Entry<String> entry = data.get(key);
        if (entry == null) {
            entry = findInFileByKey(key);
            if (entry != null) {
                data.put(entry.key(), entry);
            }
            return entry;
        }
        return entry;
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
                int keyLength = Integer.parseInt(line, 0,delimiterIndex, 10);
                String currentKey = line.substring(delimiterIndex + 1, delimiterIndex + keyLength + 1);
                if (key.equals(currentKey)) {
                    return new BaseEntry<>(currentKey, line.substring(delimiterIndex + keyLength + 1));
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
                result.append(value.key().length()).append(' ').append(value.key()).append(value.value());
                output.writeUTF(result.toString());
                result.setLength(0);
            }
            lastWritePos = output.getFilePointer();
        }
        commit = true;
    }
}
