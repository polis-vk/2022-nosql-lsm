package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private static final String LOG_NAME = "myLog";
    private static final String INDEXES_NAME = "indexes";
    private final SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> data = new ConcurrentSkipListMap<>();
    private final Config config;
    private File mapFile;
    private File indexesFile;

    public InMemoryDao() {
        config = new Config(Paths.get("tmp/" + System.currentTimeMillis()));
    }

    public InMemoryDao(Config config) {
        this.config = config;
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
    public BaseEntry<ByteBuffer> get(ByteBuffer key) {
        BaseEntry<ByteBuffer> value = data.get(key);
        if (value != null) return value;
        try {
            return getFromLog(key);
        } catch (IOException ignored) {
            return null;
        }
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        data.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    @Override
    public void flush() throws IOException {
        createFilesIfNeed();
        MapSerializeStream writer = new MapSerializeStream(mapFile, indexesFile);
        writer.serializeMap(data);
        writer.close();
    }

    private BaseEntry<ByteBuffer> getFromLog(ByteBuffer key) throws IOException {
        createFilesIfNeed();
        MapDeserializeStream reader = new MapDeserializeStream(mapFile, indexesFile);
        BaseEntry<ByteBuffer> value = reader.readByKey(key);
        reader.close();
        return value;
    }

    private void createFilesIfNeed() throws IOException {
        if (mapFile == null) {
            mapFile = new File(config.basePath().toString() + File.separator + LOG_NAME);
        }
        if (indexesFile == null) {
            indexesFile = new File(config.basePath().toString() + File.separator + INDEXES_NAME);
        }
        mapFile.createNewFile();
        indexesFile.createNewFile();
    }
}
