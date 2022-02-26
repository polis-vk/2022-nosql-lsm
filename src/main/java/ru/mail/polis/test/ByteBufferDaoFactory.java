package ru.mail.polis.test;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.alexanderkosnitskiy.InMemoryDao;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@DaoFactory
public class ByteBufferDaoFactory implements DaoFactory.Factory<ByteBuffer, BaseEntry<ByteBuffer>> {

    @Override
    public Dao<ByteBuffer, BaseEntry<ByteBuffer>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(ByteBuffer data) {
        return data == null ? null : new String(data.array(), data.arrayOffset() + data.position(),
                data.remaining(), StandardCharsets.UTF_8);
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
