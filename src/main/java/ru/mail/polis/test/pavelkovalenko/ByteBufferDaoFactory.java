package ru.mail.polis.test.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.PersistentDao;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 4, week = 1)
public class ByteBufferDaoFactory implements DaoFactory.Factory<ByteBuffer, Entry<ByteBuffer>> {

    @Override
    public Dao<ByteBuffer, Entry<ByteBuffer>> createDao(Config config) throws IOException {
        return new PersistentDao(config);
    }

    @Override
    public String toString(ByteBuffer data) {
        ByteBuffer transfer = data.asReadOnlyBuffer();
        byte[] bytes = new byte[transfer.remaining()];
        transfer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
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
