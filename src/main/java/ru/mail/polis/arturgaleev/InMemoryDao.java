package ru.mail.polis.arturgaleev;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> dataBase = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return dataBase.values().iterator();
        }
        if (from != null && to == null) {
            return dataBase.tailMap(from).values().iterator();
        }
        if (from == null) {
            return dataBase.headMap(to).values().iterator();
        }
        return dataBase.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(@NotNull BaseEntry<ByteBuffer> entry) {
        dataBase.put(entry.key(), entry);
    }
}
