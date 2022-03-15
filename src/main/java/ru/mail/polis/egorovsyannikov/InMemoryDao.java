package ru.mail.polis.egorovsyannikov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {
    ConcurrentNavigableMap<String, BaseEntry<String>> stringConcurrentSkipListMap =
            new ConcurrentSkipListMap<>(String::compareTo);

    private final Path path;
    private static final String FILE_NAME = "cache";
    private static final String NEW_LINE = "\n";

    public InMemoryDao(Config config) throws IOException {
        path = config.basePath().resolve(FILE_NAME);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        BaseEntry<String> resultFromMap = stringConcurrentSkipListMap.get(key);

        if (resultFromMap != null) {
            return resultFromMap;
        }

        if (!Files.exists(path)) {
            return null;
        }

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            for (; ; ) {
                String line = reader.readLine();
                if (line == null) {
                    return null;
                }
                if (line.equals(key)) {
                    return new BaseEntry<>(line, reader.readLine());
                }
            }
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

        List<String> entries = stringConcurrentSkipListMap
                .values()
                .stream()
                .map(entry -> entry.key() + NEW_LINE + entry.value())
                .toList();
        Files.write(path, entries);
    }
}
