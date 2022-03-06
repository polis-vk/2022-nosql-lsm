package ru.mail.polis.test.artyomdrozdov;

import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.artyomdrozdov.InMemoryDao;
import ru.mail.polis.test.DaoFactory;

@DaoFactory(stage = 1)
public class StringDaoFactory implements DaoFactory.Factory<String, Entry<String>> {

    @Override
    public Dao<String, Entry<String>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(String s) {
        return s;
    }

    @Override
    public String fromString(String data) {
        return data;
    }

    @Override
    public Entry<String> fromBaseEntry(Entry<String> baseEntry) {
        return baseEntry;
    }
}
