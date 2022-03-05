package ru.mail.polis.baidiyarosan;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private static final String DATA_FILE_NAME = "data.log";

    private final NavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> collection = new ConcurrentSkipListMap<>();

    private final Config config;

    public InMemoryDao(Config config) {
        this.config = config;
        Path path = config.basePath().resolve(DATA_FILE_NAME);
        if (Files.exists(path)) {
            try (InputStream in = Files.newInputStream(path)) {
                int size;
                while ((size = in.read()) > -1) {
                    BaseEntry<ByteBuffer> entry = new BaseEntry(ByteBuffer.wrap(in.readNBytes(size)), ByteBuffer.wrap(in.readNBytes(in.read())));
                    collection.put(entry.key(), entry);
                }
            } catch (IOException e) {
                throw new RuntimeException(e); // зато сигнатура целая
            }
        }
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (collection.isEmpty()) {
            return Collections.emptyIterator();
        }

        if (from == null && to == null) {
            return collection.values().iterator();
        }

        return collection.subMap(
                from == null ? collection.firstKey() : from, true,
                to == null ? collection.lastKey() : to, to == null
        ).values().iterator();

    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        collection.put(entry.key(), entry);
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) {
        return collection.get(key);
    }

    @Override
    public void flush() throws IOException {
        Path path = config.basePath().resolve(DATA_FILE_NAME);
        if (collection.isEmpty()) {
            return;
        }
        try (OutputStream out = Files.newOutputStream(path)) {
            for (BaseEntry<ByteBuffer> entry : collection.values()) {
                write(out, entry.key());
                write(out, entry.value());
            }
        }
    }

    private void write(OutputStream out, ByteBuffer buffer) throws IOException {
        byte[] array = buffer.array();
        out.write(array.length);
        out.write(array);
    }
}
