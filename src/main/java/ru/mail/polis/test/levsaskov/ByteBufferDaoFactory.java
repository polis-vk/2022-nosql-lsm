package ru.mail.polis.test.levsaskov;

import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.levsaskov.InMemoryDao;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 4, week = 1)
public class ByteBufferDaoFactory implements DaoFactory.Factory<ByteBuffer, Entry<ByteBuffer>> {

    @Override
    public Dao<ByteBuffer, Entry<ByteBuffer>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public Dao<ByteBuffer, Entry<ByteBuffer>> createDao(Config config) throws IOException {
        return new InMemoryDao(config);
    }

    @Override
    public String toString(ByteBuffer data) {
        return data == null ? null : new String(data.array(), StandardCharsets.UTF_8);
    }

    @Override
    public ByteBuffer fromString(String data) {
        return data == null ? null : ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<ByteBuffer> fromBaseEntry(Entry<ByteBuffer> baseEntry) {
        return baseEntry;
    }
}
