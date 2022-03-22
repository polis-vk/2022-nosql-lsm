package ru.mail.polis.baidiyarosan;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final NavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> collection = new ConcurrentSkipListMap<>();

    public InMemoryDao() throws IOException {
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        if (collection.isEmpty()) {
            return Collections.emptyIterator();
        }

        if (from == null && to == null) {
            return collection.values().iterator();
        }

        return collection.subMap(
                from == null ? collection.firstKey() : from, true,
                to == null ? collection.lastKey() : to, to == null
        ).values().iterator();

    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        collection.put(entry.key(), entry);
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {

        BaseEntry<ByteBuffer> value = collection.get(key);
        return value;
    }

    @Override
    public void flush() throws IOException {
        if (collection.isEmpty()) {
            return;
        }
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
