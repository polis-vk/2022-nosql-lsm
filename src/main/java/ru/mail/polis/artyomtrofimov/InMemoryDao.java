package ru.mail.polis.artyomtrofimov;


import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, Entry<String>> {
    private static final int MAX_CAPACITY = 20_000;
    private static final String FILENAME = File.separator + "db.txt";
    private ConcurrentNavigableMap<String, Entry<String>> data = new ConcurrentSkipListMap<>();
    private long lastPos = 0;
    private long lastWritePos = 0;
    private volatile boolean commit = false;
    private Config config = null;

    public InMemoryDao() {
    }

    public InMemoryDao(Config config) {
        this.config = config;
        loadFromFile(data, 0);
    }

    private Path getPath() {
        Path path = config.basePath();
        if (Files.isDirectory(path)) {
            path = Path.of(path + FILENAME);
            try {
                if (Files.notExists(path)) {
                    Files.createFile(path);
                }
            } catch (IOException e) {
                throw new IllegalArgumentException("Directory was provided but the file is expected;" + e);
            }
        }
        return path;
    }

    private long loadFromFile(Map<String, Entry<String>> storage, long pos) {
        try (RandomAccessFile input = new RandomAccessFile(getPath().toString(), "r")) {
            input.seek(pos);
            String line;
            while (storage.size() <= MAX_CAPACITY) {
                line = input.readUTF();
                int delimiterIndex = line.indexOf(' ');
                if (delimiterIndex == -1) {
                    continue;
                }
                Entry<String> entry = new BaseEntry<>(line.substring(0, delimiterIndex), line.substring(delimiterIndex + 1));
                storage.put(entry.key(), entry);
            }
            return input.getFilePointer();
        } catch (IOException ignored) {
            return -1;
        }
    }

    private Entry<String> findByKey(String key) {
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
                    lastPos = input.getFilePointer();
                    return new BaseEntry<>(currentKey, line.substring(delimiterIndex + 1));
                }
            }
            return null;
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public Iterator<Entry<String>> get(String from, String to) {
        boolean isFromNull = from == null;
        boolean isToNull = to == null;
        if (isFromNull && isToNull) {
            return data.values().iterator();
        }
        if (isFromNull) {
            return data.headMap(to).values().iterator();
        }
        if (isToNull) {
            if (!data.containsKey(from)) {
                ConcurrentNavigableMap<String, Entry<String>> tempStorage = new ConcurrentSkipListMap<>();
                long pos = 0;
                do {
                    tempStorage.clear();
                    pos = loadFromFile(tempStorage, pos);
                } while (pos != -1 && !tempStorage.containsKey(from));
                if (tempStorage.containsKey(from)) {
                    try {
                        flush();
                    } catch (IOException ignored) {
                    }
                    NavigableMap<String, Entry<String>> tmp = data;
                    data = tempStorage;
                    tmp.clear();
                }
            }
            return data.tailMap(from).values().iterator();
        }
        return data.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<String> get(String key) {
        if (!data.containsKey(key)) {
            Entry<String> value = findByKey(key);
            if (value != null) {
                ConcurrentNavigableMap<String, Entry<String>> tempStorage = new ConcurrentSkipListMap<>();
                loadFromFile(tempStorage, lastPos);
                try {
                    flush();
                } catch (IOException ignored) {

                }
                NavigableMap<String, Entry<String>> tmp = data;
                data = tempStorage;
                tmp.clear();
            }
            return value;
        }
        return data.get(key);
    }

    @Override
    public void upsert(Entry<String> entry) {
        data.put(entry.key(), entry);
        commit = false;
        if (config != null) {
            synchronized (data) {
                if (data.size() >= MAX_CAPACITY) {
                    try {
                        flush();
                        data.clear();
                    } catch (IOException ignored) {
                    }
                }
            }
        }
    }

    @Override
    public void flush() throws IOException {
        if (config == null || commit) {
            return;
        }
        Path path = config.basePath();
        if (Files.isDirectory(path)) {
            path = Path.of(path + FILENAME);
        }
        try (RandomAccessFile output = new RandomAccessFile(path.toString(), "rw")) {
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
