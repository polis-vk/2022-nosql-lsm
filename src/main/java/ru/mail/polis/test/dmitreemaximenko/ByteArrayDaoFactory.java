package ru.mail.polis.test.dmitreemaximenko;

import ru.mail.polis.BaseEntry;
<<<<<<< HEAD
import ru.mail.polis.Config;
=======
>>>>>>> 58b7af70ded1c7be3c9d07c8ba65091ff52723cb
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.dmitreemaximenko.InMemoryDao;
import ru.mail.polis.test.DaoFactory;
<<<<<<< HEAD

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 2, week = 1)
public class ByteArrayDaoFactory implements DaoFactory.Factory<byte[], BaseEntry<byte[]>> {

    @Override
    public Dao<byte[], BaseEntry<byte[]>> createDao() throws IOException {
=======
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 1, week = 2)
public class ByteArrayDaoFactory implements DaoFactory.Factory<byte[], BaseEntry<byte[]>> {

    @Override
    public Dao<byte[], BaseEntry<byte[]>> createDao() {
>>>>>>> 58b7af70ded1c7be3c9d07c8ba65091ff52723cb
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
<<<<<<< HEAD

    @Override
    public Dao<byte[], BaseEntry<byte[]>> createDao(Config config) throws IOException {
        return new InMemoryDao(config);
    }
=======
>>>>>>> 58b7af70ded1c7be3c9d07c8ba65091ff52723cb
}
