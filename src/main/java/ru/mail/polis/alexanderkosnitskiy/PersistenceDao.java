package ru.mail.polis.alexanderkosnitskiy;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistenceDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private static final String FILE = "data.anime";
    private static final String INDEX = "index.anime";
    private final Config config;
    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> memory = new ConcurrentSkipListMap<>();

    public PersistenceDao(Config config) {
        this.config = config;
    }

    @Override
    public BaseEntry<ByteBuffer> get(@NotNull ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> result = memory.get(key);
        if (result != null) {
            return result;
        }
        return findInFile(key);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return memory.values().iterator();
        }
        if (from == null) {
            return memory.headMap(to, false).values().iterator();
        }
        if (to == null) {
            return memory.tailMap(from, true).values().iterator();
        }
        return memory.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        memory.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        store();
        memory.clear();
    }

    private void store() throws IOException {
        try (DaoWriter out = new DaoWriter(config.basePath().resolve(FILE), config.basePath().resolve(INDEX))) {
            out.writeMap(memory);
        }
    }

    private BaseEntry<ByteBuffer> findInFile(ByteBuffer key) throws IOException {
        try (DaoReader finder = new DaoReader(config.basePath().resolve(FILE), config.basePath().resolve(INDEX))) {
            return finder.binarySearch(key);
        } catch (FileNotFoundException e) {
            return null;
        }
    }

}
