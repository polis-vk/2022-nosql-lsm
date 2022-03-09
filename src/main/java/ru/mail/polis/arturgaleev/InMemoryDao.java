package ru.mail.polis.arturgaleev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.test.arturgaleev.FileDBReader;
import ru.mail.polis.test.arturgaleev.FileDBWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> dataBase = new ConcurrentSkipListMap<>();
    private final Config config;

    public InMemoryDao() {
        config = new Config(Paths.get("tmp/temp" + Math.random()));
    }

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        if (!Files.isDirectory(config.basePath())) {
            Files.createDirectories(config.basePath());
        }
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return dataBase.values().iterator();
        }
        if (from != null && to == null) {
            return dataBase.tailMap(from).values().iterator();
        }
        if (from == null) {
            return dataBase.headMap(to).values().iterator();
        }
        return dataBase.subMap(from, to).values().iterator();
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> value = dataBase.get(key);
        if (value != null) {
            return value;
        }
        if (!Files.exists(config.basePath().resolve("1.txt"))) {
            return null;
        }
        try (FileDBReader reader = new FileDBReader(config.basePath() + "/1.txt")) {
            reader.readArrayLinks();
            return reader.getByKey(key);
        }
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        dataBase.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        try (FileDBWriter writer = new FileDBWriter(config.basePath() + "/1.txt")) {
            writer.writeMap(dataBase);
        }
    }
}
