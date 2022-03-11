package ru.mail.polis.test.artyomscheredin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.artyomscheredin.PersistentDao;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 3, week = 1)
public class DaoFactoryByteBuffer implements DaoFactory.Factory<ByteBuffer, BaseEntry<ByteBuffer>> {

    @Override
    public Dao<ByteBuffer, BaseEntry<ByteBuffer>> createDao(Config config) throws IOException {
        return new PersistentDao(config);
    }

    @Override
    public String toString(ByteBuffer data) {
        return data == null ? null : StandardCharsets.UTF_8.decode(data.asReadOnlyBuffer()).toString();
    }

    @Override
    public ByteBuffer fromString(String data) {
        return data == null ? null : StandardCharsets.UTF_8.encode(CharBuffer.wrap(data));
    }

    @Override
    public BaseEntry<ByteBuffer> fromBaseEntry(Entry<ByteBuffer> baseEntry) {
        return new BaseEntry<>(baseEntry.key(), baseEntry.value());
    }
}
