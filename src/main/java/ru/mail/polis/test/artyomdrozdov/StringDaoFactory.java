package ru.mail.polis.test.artyomdrozdov;

import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.artyomdrozdov.StringDao;
import ru.mail.polis.test.DaoFactory;

@DaoFactory
public class StringDaoFactory implements DaoFactory.Factory<String, Entry<String>> {

    @Override
    public Dao<String, Entry<String>> createDao(Config config) {
        return new StringDao(config);
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
