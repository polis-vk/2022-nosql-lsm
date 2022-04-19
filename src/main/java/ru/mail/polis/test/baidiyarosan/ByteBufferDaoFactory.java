package ru.mail.polis.test.baidiyarosan;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.baidiyarosan.MemoryAndDiskDao;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

@DaoFactory(stage = 5)
public class ByteBufferDaoFactory implements DaoFactory.Factory<ByteBuffer, BaseEntry<ByteBuffer>> {

    @Override
    public Dao<ByteBuffer, BaseEntry<ByteBuffer>> createDao(Config config) throws IOException {
        return new MemoryAndDiskDao(config);
    }

    @Override
    public String toString(ByteBuffer data) {
        if (data == null) {
            return null;
        }
        // decoder is really slow
        byte[] bytes;
        if (data.hasArray()) {
            int start = data.arrayOffset() + data.position();
            if (start == 0) {
                bytes = data.array();
            } else {
                bytes = Arrays.copyOfRange(data.array(), start, data.remaining());
            }
        } else {
            // concurrency is killing me if i don't make local copy
            ByteBuffer localData = data.duplicate();
            bytes = new byte[localData.capacity()];
            localData.get(bytes);
        }
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
