package ru.mail.polis.test.andreyilchenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.andreyilchenko.PersistentDao;
import ru.mail.polis.test.DaoFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 2)
public class ByteBufferDaoFactory implements DaoFactory.Factory<ByteBuffer, BaseEntry<ByteBuffer>> {
    @Override
    public Dao<ByteBuffer, BaseEntry<ByteBuffer>> createDao(Config config) {
        return new PersistentDao(config);
    }

    @Override
    public String toString(ByteBuffer data) {
        if (data == null) {
            return null;
        }
        int dataCurrentPosition = data.arrayOffset() + data.position();
        return new String(data.array(), dataCurrentPosition, data.remaining(), StandardCharsets.UTF_8);
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
