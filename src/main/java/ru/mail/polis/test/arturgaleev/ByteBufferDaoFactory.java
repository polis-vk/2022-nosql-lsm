package ru.mail.polis.test.arturgaleev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.arturgaleev.InMemoryDao;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 3, week = 1)
public class ByteBufferDaoFactory implements DaoFactory.Factory<ByteBuffer, BaseEntry<ByteBuffer>> {

    @Override
    public Dao<ByteBuffer, BaseEntry<ByteBuffer>> createDao(Config config) throws IOException {
        return new InMemoryDao(config);
    }

    @Override
    public String toString(ByteBuffer in) {
        if (in == null) {
            return null;
        }
        ByteBuffer data = in.asReadOnlyBuffer();
        byte[] bytes = new byte[data.limit()];
        data.position(0);
        data.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public ByteBuffer fromString(String data) {
        return data == null ? null : ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public BaseEntry<ByteBuffer> fromBaseEntry(Entry<ByteBuffer> baseEntry) {
        return new BaseEntry<>(baseEntry.key(), baseEntry.value());
    }
}
