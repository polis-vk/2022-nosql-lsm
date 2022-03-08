package ru.mail.polis.test.daniilbakin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.daniilbakin.InMemoryDao;
import ru.mail.polis.test.DaoFactory;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 2)
public class ByteBufferDaoFactory implements DaoFactory.Factory<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();

    @Override
    public Dao<ByteBuffer, BaseEntry<ByteBuffer>> createDao(Config config) {
        return new InMemoryDao(config);
    }

    @Override
    public Dao<ByteBuffer, BaseEntry<ByteBuffer>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(ByteBuffer data) {
        if (data == null) return null;
        if (data.hasArray()) {
            data.position(data.arrayOffset());
            return new String(data.array(), StandardCharsets.UTF_8);
        }
        try {
            return decoder.decode(data).toString();
        } catch (CharacterCodingException e) {
            return null;
        }
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
