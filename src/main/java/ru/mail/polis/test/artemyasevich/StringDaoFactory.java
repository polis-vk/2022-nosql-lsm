package ru.mail.polis.test.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.artemyasevich.InMemoryDao;
import ru.mail.polis.test.DaoFactory;
@DaoFactory
public class StringDaoFactory implements DaoFactory.Factory<String, BaseEntry<String>> {
    @Override
    public Dao<String, BaseEntry<String>> createDao() {
        return new InMemoryDao();
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
