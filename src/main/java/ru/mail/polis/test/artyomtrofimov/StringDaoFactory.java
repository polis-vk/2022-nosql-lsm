package ru.mail.polis.test.artyomtrofimov;

import java.io.IOException;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.artyomtrofimov.InMemoryDao;
import ru.mail.polis.test.DaoFactory;

@DaoFactory(stage = 2)
public class StringDaoFactory implements DaoFactory.Factory<String, Entry<String>> {

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

    @Override
    public Dao<String, Entry<String>> createDao(Config config) throws IOException {
        return new InMemoryDao(config);
    }
}
