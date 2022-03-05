package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private static final String KEY_VALUE_SEPARATOR = ":";
    private Path path;
    private final NavigableMap<String, BaseEntry<String>> data = new ConcurrentSkipListMap<>();

    public InMemoryDao() {
    }

    public InMemoryDao(Config config) {
        path = Path.of(config.basePath() + File.separator + "daoData.txt");
        try (Stream<String> lines = Files.lines(path, Charset.defaultCharset())) {
            lines.forEach(line -> {
                String[] keyValue = line.split(KEY_VALUE_SEPARATOR);
                data.put(keyValue[0], new BaseEntry<>(keyValue[0], keyValue[1]));
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) {
        if (from == null && to == null) {
            return data.values().iterator();
        }
        if (from == null) {
            return data.headMap(to).values().iterator();
        }
        if (to == null) {
            return data.tailMap(from).values().iterator();
        }
        return data.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        data.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        try {
            if (!Files.exists(path)) {
                Files.createFile(path);
            }
            String stingToWrite;
            for (BaseEntry<String> baseEntry : data.values()) {
                stingToWrite = baseEntry.key() + KEY_VALUE_SEPARATOR + baseEntry.value() + '\n';
                Files.writeString(path, stingToWrite, Charset.defaultCharset(), StandardOpenOption.APPEND);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
