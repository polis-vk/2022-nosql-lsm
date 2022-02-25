package ru.mail.polis.test.artyomscheredin;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.artyomscheredin.inMemoryDAO;
import ru.mail.polis.test.DaoFactory;

@DaoFactory
public class DaoFactoryString implements DaoFactory.Factory<ByteBuffer, BaseEntry<ByteBuffer>> {
    @Override
    public Dao<ByteBuffer, BaseEntry<ByteBuffer>> createDao() {
        return new inMemoryDAO();
    }

    @Override
    public String toString(ByteBuffer data) {
        if (data == null) {
            return null;
        }
        return new String(data.array(), StandardCharsets.UTF_8);
    }

    @Override
    public ByteBuffer fromString(String data) {
        if (data == null) {
            return null;
        }
        return ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public BaseEntry<ByteBuffer> fromBaseEntry(Entry<ByteBuffer> baseEntry) {
        return new BaseEntry<>(baseEntry.key(), baseEntry.value());
    }
}
