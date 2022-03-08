package ru.mail.polis.alexandratkachenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private final ConcurrentSkipListMap<ByteBuffer, BaseEntry<ByteBuffer>> map = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        ConcurrentMap<ByteBuffer, BaseEntry<ByteBuffer>> result;
        if (from == null && to == null) {
            result = map;
        } else if (from == null) {
            result = map.headMap(to);
        } else if (to == null) {
            result = map.tailMap(from);
        } else {
            result = map.subMap(from, to);
        }
        return result.values().iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        map.put(entry.key(), entry);
    }
}

