package ru.mail.polis.test;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

@DaoFactory
public class StringDaoFactory implements DaoFactory.Factory<String, BaseEntry<String>> {
    @Override
    public Dao<String, BaseEntry<String>> createDao() {
        return null;
    }

    @Override
    public String toString(String data) {
        return null;
    }

    @Override
    public String fromString(String data) {
        return null;
    }

    @Override
    public BaseEntry<String> fromBaseEntry(Entry<String> baseEntry) {
        return null;
    }

    @Override
    public Dao<String, Entry<String>> createStringDao() {
        return DaoFactory.Factory.super.createStringDao();
    }
}
