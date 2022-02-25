package ru.mail.polis.baidiyarosan;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final ConcurrentNavigableMap<ByteBuffer, ByteBuffer> collection = new ConcurrentSkipListMap<>();

    private final Function<Map.Entry<ByteBuffer, ByteBuffer>, BaseEntry<ByteBuffer>> mapper =
            (e) -> new BaseEntry(e.getKey(), e.getValue());

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return collection.entrySet().stream().map(mapper).iterator();
        }

        return collection.subMap(
                from == null ? collection.firstKey() : from, true,
                to == null ? collection.lastKey() : to, to == null
        ).entrySet().stream().map(mapper).iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        collection.put(entry.key(), entry.value());
    }

}
