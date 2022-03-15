package ru.mail.polis.test.artyomdrozdov;

import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.artyomdrozdov.ByteBufferDao;
import ru.mail.polis.test.DaoFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

@DaoFactory
public class ByteBufferDaoFactory implements DaoFactory.Factory<ByteBuffer, Entry<ByteBuffer>> {

    @Override
    public Dao<ByteBuffer, Entry<ByteBuffer>> createDao() {
        return new ByteBufferDao();
    }

    @Override
    public String toString(ByteBuffer s) {
        // TODO optimize
        return s == null ? null : StandardCharsets.UTF_8.decode(s.asReadOnlyBuffer()).toString();
    }

    @Override
    public ByteBuffer fromString(String data) {
        return data == null ? null : StandardCharsets.UTF_8.encode(CharBuffer.wrap(data));
    }

    @Override
    public Entry<ByteBuffer> fromBaseEntry(Entry<ByteBuffer> baseEntry) {
        return baseEntry;
    }
}
