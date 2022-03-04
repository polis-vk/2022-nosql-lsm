package ru.mail.polis.test.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.InMemoryDao;
import ru.mail.polis.test.DaoFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 2, week = 1)
public class ByteBufferDaoFactory implements DaoFactory.Factory<ByteBuffer, BaseEntry<ByteBuffer>> {

    @Override
    public Dao<ByteBuffer, BaseEntry<ByteBuffer>> createDao(Config config) {
        return new InMemoryDao(config);
    }

    @Override
    public String toString(ByteBuffer data) {
        return new String(data.array(), StandardCharsets.UTF_8);
    }

    @Override
    public ByteBuffer fromString(String data) {
        return data == null ? null : ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public BaseEntry<ByteBuffer> fromBaseEntry(Entry<ByteBuffer> baseEntry) {
        return new BaseEntry<>(baseEntry.key(), baseEntry.value());
    }

    public Config extractConfig(Dao<ByteBuffer, BaseEntry<ByteBuffer>> dao) {
        return ((InMemoryDao)dao).getConfig();
    }

    public Dao<ByteBuffer, BaseEntry<ByteBuffer>> reopen(Dao<ByteBuffer, BaseEntry<ByteBuffer>> dao) {
        return ((InMemoryDao)dao).reopen();
    }

}
