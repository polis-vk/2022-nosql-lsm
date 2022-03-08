package ru.mail.polis.test.dmitreemaximenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.dmitreemaximenko.InMemoryDao;
import ru.mail.polis.test.DaoFactory;
import java.nio.charset.StandardCharsets;

<<<<<<< HEAD
@DaoFactory
=======
@DaoFactory(stage = 2, week = 1)
>>>>>>> 4224b8a... Half of changes implemented
public class ByteArrayDaoFactory implements DaoFactory.Factory<byte[], BaseEntry<byte[]>> {

    @Override
    public Dao<byte[], BaseEntry<byte[]>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(byte[] data) {
        return data == null ? null : new String(data, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] fromString(String data) {
        return data == null ? null : data.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public BaseEntry<byte[]> fromBaseEntry(Entry<byte[]> baseEntry) {
        return new BaseEntry<>(baseEntry.key(), baseEntry.value());
    }

    @Override
    public Dao<byte[], BaseEntry<byte[]>> createDao(Config config) {
        return new InMemoryDao(config);
    }
}
