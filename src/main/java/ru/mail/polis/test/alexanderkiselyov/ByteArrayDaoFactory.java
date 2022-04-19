package ru.mail.polis.test.alexanderkiselyov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.alexanderkiselyov.InMemoryDao;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 5, week = 2)
public class ByteArrayDaoFactory implements DaoFactory.Factory<Byte[], BaseEntry<Byte[]>> {
    @Override
    public Dao<Byte[], BaseEntry<Byte[]>> createDao(Config config) throws IOException {
        return new InMemoryDao(config);
    }

    @Override
    public String toString(Byte[] data) {
        if (data == null) {
            return null;
        }
        byte[] dataPrimitive = new byte[data.length];
        int index = 0;
        for (Byte d : data) {
            dataPrimitive[index++] = d;
        }
        return new String(dataPrimitive, StandardCharsets.UTF_8);
    }

    @Override
    public Byte[] fromString(String data) {
        if (data == null) {
            return null;
        }
        Byte[] dataByte = new Byte[data.length()];
        int index= 0;
        for (byte b : data.getBytes(StandardCharsets.UTF_8)) {
            dataByte[index++] = b;
        }
        return dataByte;
    }

    @Override
    public BaseEntry<Byte[]> fromBaseEntry(Entry<Byte[]> baseEntry) {
        return new BaseEntry<>(baseEntry.key(), baseEntry.value());
    }
}
