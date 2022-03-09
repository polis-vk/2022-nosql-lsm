package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private static final String FILE_NAME = "storage.txt";
    private final ConcurrentNavigableMap<String, BaseEntry<String>> storage = new ConcurrentSkipListMap<>();
    private final DaoReader reader;
    private final DaoWriter writer;

    public InMemoryDao(Config config) {
        Path pathToFile = config.basePath().resolve(FILE_NAME);
        try {
            if (!Files.exists(config.basePath())) {
                Files.createDirectories(config.basePath());
            }
            if (!Files.exists(pathToFile)) {
                Files.createFile(pathToFile);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.reader = new DaoReader(pathToFile);
        this.writer = new DaoWriter(pathToFile);
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        Collection<BaseEntry<String>> values;
        if (from == null && to == null) {
            values = storage.values();
        } else if (from == null) {
            values = storage.headMap(to).values();
        } else if (to == null) {
            values = storage.tailMap(from).values();
        } else {
            values = storage.subMap(from, to).values();
        }
        return values.iterator();
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        BaseEntry<String> value;
        if (storage.containsKey(key)) {
            value = storage.get(key);
        } else {
            value = reader.findEntryByKey(key);
        }
        return value;
    }

    @Override
    public void flush() throws IOException {
        writer.writeDAO(storage);
        storage.clear();
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        storage.put(entry.key(), entry);
    }
}
