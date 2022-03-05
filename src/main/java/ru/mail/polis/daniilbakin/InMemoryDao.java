package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> data;
    private Config config;

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
        return data.get(key);
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        data.put(entry.key(), entry);
    }

    public InMemoryDao() {
        data = new ConcurrentSkipListMap<>();
    }

    public InMemoryDao(Config config) {
        this.config = config;
        SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> map;
        try {
            map = initializeFromConfig();
        } catch (IOException e) {
            e.printStackTrace();
            map = new ConcurrentSkipListMap<>();
        }
        data = map;
    }

    private SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> initializeFromConfig() throws IOException {
        File file = new File(config.basePath().toString() + File.separator + "myLog");
        file.createNewFile();
        MapInputStream reader = new MapInputStream(file);
        SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> map = reader.readMap();
        reader.close();
        return map;
    }

    @Override
    public void flush() throws IOException {
        MapOutputStream writer = new MapOutputStream(config.basePath().toString() + File.separator + "myLog");
        writer.writeMap(data);
        writer.close();
    }
}
