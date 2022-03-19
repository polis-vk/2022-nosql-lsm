package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> data = new ConcurrentSkipListMap<>();
    private final Config config;
    private final int dataCounter;
    private final MapsDeserializeStream deserialize;

    public InMemoryDao(Config config) throws IOException {
        dataCounter = countDataFiles(config);
        this.config = config;
        deserialize = new MapsDeserializeStream(config, dataCounter);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        return deserialize.getRange(from, to, new PeekIterator<>(getDataIterator(from, to)));
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> value = data.get(key);
        if (value != null) {
            return value.value() == null ? null : value;
        }
        if (dataCounter == 0) {
            return null;
        }
        return deserialize.readByKey(key);
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        data.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        deserialize.close();
        flush();
    }

    @Override
    public void flush() throws IOException {
        MapSerializeStream writer = new MapSerializeStream(config, dataCounter);
        writer.serializeMap(data);
        writer.close();
    }

    private Iterator<BaseEntry<ByteBuffer>> getDataIterator(ByteBuffer from, ByteBuffer to) {
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

    private int countDataFiles(Config config) throws IOException {
        try (Stream<Path> files = Files.list(config.basePath())) {
            return (int) files.count() / 2;
        } catch (NoSuchFileException e) {
            return 0;
        }
    }
}
