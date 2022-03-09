package ru.mail.polis.levsaskov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private final ConcurrentSkipListMap<ByteBuffer, BaseEntry<ByteBuffer>> entrys = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        Iterator<BaseEntry<ByteBuffer>> ans;

        if (from == null && to == null) {
            ans = entrys.values().iterator();
        } else if (from == null) {
            ans = entrys.headMap(to).values().iterator();
        } else if (to == null) {
            ans = entrys.tailMap(from).values().iterator();
        } else {
            ans = entrys.subMap(from, to).values().iterator();
        }

        return ans;
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        entrys.put(entry.key(), entry);
    }
}
