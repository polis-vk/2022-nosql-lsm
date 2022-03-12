package ru.mail.polis.test.stepanponomarev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.stepanponomarev.InMemoryDao;
import ru.mail.polis.test.DaoFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@DaoFactory
public class ByteBufferDaoFactory implements DaoFactory.Factory<ByteBuffer, Entry<ByteBuffer>> {

    @Override
    public Dao<ByteBuffer, Entry<ByteBuffer>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(ByteBuffer data) {
        if (data == null) {
            return null;
        }

        if (!data.hasArray()) {
            throw new IllegalArgumentException("Buffer should have array");
        }

        int startIndex = data.arrayOffset();
        int curIndex = data.arrayOffset() + data.position();
        int endIndex = curIndex + data.remaining();

        return new String(data.array(), startIndex, endIndex, StandardCharsets.UTF_8);
    }

    @Override
    public ByteBuffer fromString(String data) {
        if (data == null) {
            return null;
        }

        return ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<ByteBuffer> fromBaseEntry(Entry<ByteBuffer> entry) {
        return new BaseEntry<>(entry.key(), entry.value());
    }
}
