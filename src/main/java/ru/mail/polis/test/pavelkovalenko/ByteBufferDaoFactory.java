package ru.mail.polis.test.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.InMemoryDao;
import ru.mail.polis.test.DaoFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

@DaoFactory
public class ByteBufferDaoFactory implements DaoFactory.Factory<ByteBuffer, BaseEntry<ByteBuffer>> {
    @Override
    public Dao<ByteBuffer, BaseEntry<ByteBuffer>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(ByteBuffer data) {
        return new String(data.array(), Charset.defaultCharset());
    }

    @Override
    public ByteBuffer fromString(String data) {
        return data == null ? null : ByteBuffer.wrap(data.getBytes(Charset.defaultCharset()));
    }

    @Override
    public BaseEntry<ByteBuffer> fromBaseEntry(Entry<ByteBuffer> baseEntry) {
        return new BaseEntry<>(baseEntry.key(), baseEntry.value());
    }
}
