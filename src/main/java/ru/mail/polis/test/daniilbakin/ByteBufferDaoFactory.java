package ru.mail.polis.test.daniilbakin;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.daniilbakin.InMemoryDao;
import ru.mail.polis.test.DaoFactory;

import static java.nio.ByteBuffer.wrap;

@DaoFactory
public class ByteBufferDaoFactory implements DaoFactory.Factory<ByteBuffer, BaseEntry<ByteBuffer>> {
    @Override
    public Dao<ByteBuffer, BaseEntry<ByteBuffer>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(ByteBuffer data) {
        if (data == null) return null;
        data.position(data.arrayOffset());
        return new String(data.array(), StandardCharsets.UTF_8);
    }

    @Override
    public ByteBuffer fromString(String data) {
        return data == null ? null : wrap(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public BaseEntry<ByteBuffer> fromBaseEntry(Entry<ByteBuffer> baseEntry) {
        return new BaseEntry<>(baseEntry.key(), baseEntry.value());
    }
}
