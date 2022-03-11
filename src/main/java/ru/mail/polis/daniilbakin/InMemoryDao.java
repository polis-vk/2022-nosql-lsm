package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> data = new ConcurrentSkipListMap<>();
    private final Path mapPath;
    private final Path indexesPath;
    private MapDeserializeStream deserialize;
    private boolean dataNotExistsForSure;

    public InMemoryDao(Config config) {
        this.mapPath = config.basePath().resolve("myLog");
        this.indexesPath = config.basePath().resolve("indexes");
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
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
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> value = data.get(key);
        if (value != null) {
            return value;
        }
        if (dataNotExistsForSure) {
            return null;
        }
        return getFromLog(key);
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        data.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        if (deserialize != null) {
            deserialize.close();
        }
        flush();
    }

    @Override
    public void flush() throws IOException {
        MapSerializeStream writer = new MapSerializeStream(mapPath, indexesPath);
        writer.serializeMap(data);
        writer.close();
    }

    private BaseEntry<ByteBuffer> getFromLog(ByteBuffer key) throws IOException {
        if (deserialize == null) {
            try {
                deserialize = new MapDeserializeStream(mapPath, indexesPath);
            } catch (NoSuchFileException e) {
                dataNotExistsForSure = true;
                return null;
            }
        }
        return deserialize.readByKey(key);
    }
}
