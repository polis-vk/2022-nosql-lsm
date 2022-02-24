package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private final NavigableSet<BaseEntry<ByteBuffer>> storage =
            new ConcurrentSkipListSet<>(Comparator.comparing(BaseEntry::key));

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null) {
            return storage.iterator();
        }
        if (to == null) {
            return storage.tailSet(new BaseEntry<>(from, null), true).iterator();
        }
        return storage.subSet(new BaseEntry<>(from, null), true, new BaseEntry<>(to, null), false)
                .iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        storage.add(entry);
    }

}
