package ru.mail.polis.test.baidiyarosan;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.baidiyarosan.FileUtils;
import ru.mail.polis.baidiyarosan.InMemoryDao;
import ru.mail.polis.baidiyarosan.MemoryAndDiskDao;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 3, week = 2)
public class ByteBufferDaoFactory implements DaoFactory.Factory<ByteBuffer, BaseEntry<ByteBuffer>> {

    @Override
    public Dao<ByteBuffer, BaseEntry<ByteBuffer>> createDao(Config config) throws IOException {
        // speed up cases when no data is saved on disk
        // TODO remove
        if (FileUtils.getPaths(config.basePath()).size() == 0) {
           return new InMemoryDao(config);
        }

        return new MemoryAndDiskDao(config);
    }

    @Override
    public String toString(ByteBuffer data) {
        return data == null ? null : StandardCharsets.UTF_8.decode(data.asReadOnlyBuffer()).toString();
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
