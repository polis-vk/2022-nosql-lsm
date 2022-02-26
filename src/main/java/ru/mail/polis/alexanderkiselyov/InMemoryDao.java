package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {
    Comparator<byte[]> byteArrayComparator = Arrays::compare;
    NavigableMap<byte[], BaseEntry<byte[]>> pairs = new ConcurrentSkipListMap<>(byteArrayComparator);

    @Override
    public Iterator<BaseEntry<byte[]>> get(byte[] from, byte[] to) {
        if (from == null && to == null) {
            return pairs.values().iterator();
        } else if (from == null) {
            return pairs.headMap(to).values().iterator();
        } else if (to == null) {
            return pairs.tailMap(from).values().iterator();
        }
        return pairs.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(BaseEntry<byte[]> entry) {
        pairs.put(entry.key(), entry);
    }
}
