package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.util.Iterator;

public class InMemoryDao<D, E extends Entry<D>> implements Dao<D, E> {
    @Override
    public Iterator<E> get(D from, D to) {
        return null;
    }

    @Override
    public E get(D key) {
        return Dao.super.get(key);
    }

    @Override
    public Iterator<E> allFrom(D from) {
        return Dao.super.allFrom(from);
    }

    @Override
    public Iterator<E> allTo(D to) {
        return Dao.super.allTo(to);
    }

    @Override
    public Iterator<E> all() {
        return Dao.super.all();
    }

    @Override
    public void upsert(E entry) {

    }
}
