package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> data = new ConcurrentSkipListMap<>();

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
    public void upsert(BaseEntry<ByteBuffer> entry) {
        data.put(entry.key(), entry);
    }
}
