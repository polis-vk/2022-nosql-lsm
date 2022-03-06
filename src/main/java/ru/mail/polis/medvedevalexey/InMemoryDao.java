package ru.mail.polis.medvedevalexey;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {

    private final ConcurrentNavigableMap<byte[], BaseEntry<byte[]>> storage =
            new ConcurrentSkipListMap<>(Arrays::compare);

    @Override
    public Iterator<BaseEntry<byte[]>> get(byte[] from, byte[] to) {
        if (from == null && to == null) {
            return storage.values().iterator();
        }
        if (from == null) {
            return storage.headMap(to).values().iterator();
        }
        if (to == null) {
            return storage.tailMap(from).values().iterator();
        }
        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(BaseEntry<byte[]> entry) {
        if (entry == null) {
            throw new IllegalArgumentException();
        }
        storage.put(entry.key(), entry);
    }
}
