package ru.mail.polis.egorovsyannikov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {
    ConcurrentNavigableMap<String, BaseEntry<String>> stringConcurrentSkipListMap =
            new ConcurrentSkipListMap<>(String::compareTo);

    private final Path path;
    private static final String FILE_NAME = "cache";

    public InMemoryDao(Config config) throws IOException {
        path = config.basePath().resolve(FILE_NAME);
    }

    @Override
    public BaseEntry<String> get(String key) {
        BaseEntry<String> resultFromMap = stringConcurrentSkipListMap.get(key);

        if (resultFromMap != null) {
            return resultFromMap;
        }

        if (!Files.exists(path)) {
            return null;
        }

        try (DataInputStream reader = new DataInputStream(Files.newInputStream(path))) {
            for (; ; ) {
                BaseEntry<String> entry = new BaseEntry<>(reader.readUTF(), reader.readUTF());
                if (entry.key().equals(key)) {
                    return entry;
                }
            }
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) {
        if (from == null && to == null) {
            return getIterator(stringConcurrentSkipListMap);
        }
        if (from == null) {
            return getIterator(stringConcurrentSkipListMap.headMap(to));
        }
        if (to == null) {
            return getIterator(stringConcurrentSkipListMap.tailMap(from, true));
        }
        return getIterator(stringConcurrentSkipListMap.subMap(from, to));
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        stringConcurrentSkipListMap.put(entry.key(), entry);
    }

    private static Iterator<BaseEntry<String>> getIterator(ConcurrentNavigableMap<String, BaseEntry<String>> map) {
        return map.values().iterator();
    }

    @Override
    public void flush() throws IOException {
        if (!Files.exists(path)) {
            Files.createFile(path);
        }

        try (DataOutputStream writer = new DataOutputStream(Files.newOutputStream(path))) {
            for(BaseEntry<String> entry: stringConcurrentSkipListMap.values()) {
                writer.writeUTF(entry.key());
                writer.writeUTF(entry.value());
            }
        }
    }
}
