package ru.mail.polis.artyomdrozdov;

import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class ByteBufferDao implements Dao<ByteBuffer, Entry<ByteBuffer>> {

    private static final ByteBuffer VERY_FIRST_KEY = ByteBuffer.allocateDirect(0); // ???

    private final ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> storage = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null) {
            from = VERY_FIRST_KEY;
        }

        if (to == null) {
            return storage.tailMap(from).values().iterator();
        }

        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<ByteBuffer> get(ByteBuffer key) {
        return storage.get(key);
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        storage.put(entry.key(), entry);
    }
}
