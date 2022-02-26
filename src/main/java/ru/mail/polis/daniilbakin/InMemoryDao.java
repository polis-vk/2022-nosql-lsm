package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final SortedSet<BaseEntry<ByteBuffer>> data =
            new ConcurrentSkipListSet<>(Comparator.comparing(BaseEntry::key));

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return data.iterator();
        }
        if (from == null) {
            return data.headSet(new BaseEntry<>(to, null)).iterator();
        }
        if (to == null) {
            return data.tailSet(new BaseEntry<>(from, null)).iterator();
        }
        return data.subSet(new BaseEntry<>(from, null), new BaseEntry<>(to, null)).iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        data.add(entry);
    }

}


