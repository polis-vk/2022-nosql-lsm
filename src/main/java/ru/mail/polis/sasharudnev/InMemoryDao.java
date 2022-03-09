package ru.mail.polis.sasharudnev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private final ConcurrentNavigableMap<String, BaseEntry<String>> data = new ConcurrentSkipListMap<>();
    private final String path;
    private static final String SEPARATOR = " ";

    public InMemoryDao(Config config) throws IOException {
        path = config.basePath().resolve("data.txt").toString();
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) {
        Map<String, BaseEntry<String>> dataSet;
        boolean isFromEqualsNull = from == null;
        boolean isToEqualsNull = to == null;

        if (isFromEqualsNull && isToEqualsNull) {
            dataSet = data;
        } else if (isFromEqualsNull) {
            dataSet = data.headMap(to);
        } else if (isToEqualsNull) {
            dataSet = data.tailMap(from);
        } else {
            dataSet = data.subMap(from, to);
        }

        return dataSet.values().iterator();
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        if (data.containsKey(key)) {
            return data.get(key);
        }
        if (Files.exists(Path.of(path))) {
            try (BufferedReader reader = Files.newBufferedReader(Path.of(path))) {
                String line = reader.readLine();
                while (line != null) {
                    String[] entry = line.split(SEPARATOR, -1);
                    if (entry[0].equals(key)) {
                        return new BaseEntry<>(entry[0], entry[1]);
                    }
                    line = reader.readLine();
                }
            } catch (IOException de) {
                throw new IOException(de.getMessage());
            }
        }
        return null;
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        data.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(path), StandardCharsets.UTF_8)) {
            if (Files.notExists(Path.of(path))) {
                Files.createFile(Path.of(path));
            }
            for (Map.Entry<String, BaseEntry<String>> entry : data.entrySet()) {
                String builder = entry.getKey()
                        + SEPARATOR
                        + entry.getValue().value()
                        + "\n";
                writer.write(builder);
            }
        } catch (Exception de) {
            throw new IOException(de.getMessage());
        }
    }
}
