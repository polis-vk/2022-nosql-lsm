package ru.mail.polis.andreyilchenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> entries = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (entries.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (from == null) {
            from = entries.firstKey();
        }
        if (to == null) {
            return entries.subMap(from, true, entries.lastKey(), true).values().iterator();
        }
        return entries.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        entries.put(entry.key(), entry);
    }
}
