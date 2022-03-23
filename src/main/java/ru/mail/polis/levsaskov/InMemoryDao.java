package ru.mail.polis.levsaskov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> entrys = new ConcurrentSkipListMap<>();
    private StorageSystem storageSystem;

    public InMemoryDao() {
        storageSystem = null;
    }

    public InMemoryDao(Config config) throws IOException {
        storageSystem = new StorageSystem();

        if (!storageSystem.init(config.basePath())) {
            storageSystem = null;
        }
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> localEntry = entrys.get(key);
        if (storageSystem != null && localEntry == null) {
            return storageSystem.findEntry(key);
        }

        return localEntry;
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> local;

        if (from == null && to == null) {
            local = entrys;
        } else if (from == null) {
            local = entrys.headMap(to);
        } else if (to == null) {
            local = entrys.tailMap(from);
        } else {
            local = entrys.subMap(from, to);
        }

        return storageSystem == null ? local.values().iterator() :
                storageSystem.getMergedEntrys(local, from, to).values().iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        entrys.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (storageSystem != null) {
            storageSystem.save(entrys);
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        storageSystem.close();
    }
}
