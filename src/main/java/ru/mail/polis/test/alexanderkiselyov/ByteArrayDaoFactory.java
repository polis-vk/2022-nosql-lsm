package ru.mail.polis.test.alexanderkiselyov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.alexanderkiselyov.InMemoryDao;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 3, week = 1)
public class ByteArrayDaoFactory implements DaoFactory.Factory<byte[], BaseEntry<byte[]>> {
    @Override
    public Dao<byte[], BaseEntry<byte[]>> createDao(Config config) throws IOException {
        return new InMemoryDao(config);
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
}
