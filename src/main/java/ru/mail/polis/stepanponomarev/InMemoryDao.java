package ru.mail.polis.stepanponomarev;

import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, Entry<ByteBuffer>> {
    private final SortedMap<ByteBuffer, Entry<ByteBuffer>> store = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return store.values().iterator();
        }

        if (from == null) {
            return store.headMap(to).values().iterator();
        }

        if (to == null) {
            return store.tailMap(from).values().iterator();
        }

        return store.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        if (entry == null) {
            throw new NullPointerException("Entry can't be null");
        }

        store.put(entry.key(), entry);
    }
}
