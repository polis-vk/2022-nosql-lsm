package ru.mail.polis.test.lutsenkodmitrii;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.lutsenkodmitrii.InMemoryDao;
import ru.mail.polis.test.DaoFactory;

@DaoFactory(stage = 3, week = 1)
public class StringDaoFactory implements DaoFactory.Factory<String, BaseEntry<String>> {

    @Override
    public Dao<String, BaseEntry<String>> createDao(Config config) {
        return new InMemoryDao(config);
    }

    @Override
    public String toString(String data) {
        return data;
    }

    @Override
    public String fromString(String data) {
        return data;
    }

    @Override
    public BaseEntry<String> fromBaseEntry(Entry<String> baseEntry) {
        return new BaseEntry<>(baseEntry.key(), baseEntry.value());
    }
}
