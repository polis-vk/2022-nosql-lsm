package ru.mail.polis.artyomtrofimov;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;


public class InMemoryDao implements Dao<String, Entry<String>> {
    private ConcurrentNavigableMap<String, Entry<String>> data = new ConcurrentSkipListMap<>();
    private Config config = null;

    public InMemoryDao() {
    }

    public InMemoryDao(Config config) {
        this.config = config;
        Path path = config.basePath();
        if (Files.isDirectory(config.basePath())) {
            try {
                Files.createFile(Path.of(path + File.separator + "db"));
            } catch (IOException e) {
                throw new IllegalArgumentException("Directory was provided but the file is expected");
            }

        }
        if (Files.exists(path)) {
            try (Stream<String> stream = Files.lines(path)) {
                Map<String, Entry<String>> map = stream
                        .map(e -> {
                            int delimeterIndex = e.indexOf(':');
                            return new BaseEntry<>(e.substring(0, delimeterIndex), e.substring(delimeterIndex + 1));
                        })
                        .collect(Collectors.toMap(k -> k.key(), v -> v));
                data = new ConcurrentSkipListMap<>(map);
            } catch (IOException e) {
                data = new ConcurrentSkipListMap<>();
            }
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
            return data.tailMap(from).values().iterator();
        }
        return data.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<String> entry) {
        data.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        Path path = config.basePath();
        if (Files.isDirectory(path)) {
            throw new IllegalArgumentException("Directory was provided but the file is expected");
        }
        try (RandomAccessFile output = new RandomAccessFile(path.toString(), "rws")) {
            StringBuilder result = new StringBuilder();
            for (Entry<String> value : data.values()) {
                result.append(value.key()).append(':').append(value.value()).append('\n');
                output.writeChars(result.toString());
                result.setLength(0);
            }
        }
    }
}
