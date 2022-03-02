package ru.mail.polis.artyomdrozdov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.Collections;
import java.util.Iterator;

public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {
    @Override
    public Iterator<BaseEntry<byte[]>> get(byte[] from, byte[] to) {
        return Collections.emptyIterator();
    }

    @Override
    public void upsert(BaseEntry<byte[]> entry) {

    }
}
