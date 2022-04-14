package ru.mail.polis.levsaskov;

import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, Entry<ByteBuffer>> {
    private final ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> entrys = new ConcurrentSkipListMap<>();
    private final StorageSystem storageSystem;

    public InMemoryDao() {
        storageSystem = null;
    }

    public InMemoryDao(Config config) throws IOException {
        storageSystem = StorageSystem.load(config.basePath());
    }

    @Override
    public Entry<ByteBuffer> get(ByteBuffer key) throws IOException {
        Entry<ByteBuffer> ans = entrys.get(key);
        if (storageSystem != null && ans == null) {
            ans = storageSystem.findEntry(key);
        }
        if (ans == null || ans.value() == null) {
            return null;
        }

        return ans;
    }

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> local;

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
                storageSystem.getMergedEntrys(local, from, to);
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        entrys.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (storageSystem != null) {
            storageSystem.save(entrys);
            entrys.clear();
        }
    }

    @Override
    public void compact() throws IOException {
        if(storageSystem.isCompacted() && entrys.isEmpty()) {
            return;
        }

        storageSystem.compact(entrys);
        entrys.clear();
    }

    @Override
    public void close() throws IOException {
        flush();
        storageSystem.close();
    }
}
