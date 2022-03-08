package ru.mail.polis.baidiyarosan;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

    private final Path path;

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        this.path = config.basePath().resolve(DATA_FILE_NAME);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
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
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {

        BaseEntry<ByteBuffer> value = collection.get(key);
        if (value != null) {
            return value;
        }
        if (!Files.exists(path)) {
            return null;
        }
        return searchFile(key);
    }

    private BaseEntry<ByteBuffer> searchFile(ByteBuffer key) throws IOException {
        try (InputStream in = Files.newInputStream(path)) {
            while (in.available() > Integer.BYTES) {
                ByteBuffer value = read(in, key);
                if (value != null) {
                    return new BaseEntry<>(key, value);
                }

            }
        }
        return null;
    }

    private ByteBuffer read(InputStream in, ByteBuffer key) throws IOException {
        int keySize = in.read();
        if (!key.equals(wrap(in.readNBytes(keySize)))) {
            return null;
        }
        int valSize = in.read();
        return ByteBuffer.wrap(in.readNBytes(valSize));
    }

    //non-heap?
    private ByteBuffer wrap(byte[] bytes) {
        return ByteBuffer.allocateDirect(bytes.length).put(bytes).flip();
    }

    @Override
    public void flush() throws IOException {
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
