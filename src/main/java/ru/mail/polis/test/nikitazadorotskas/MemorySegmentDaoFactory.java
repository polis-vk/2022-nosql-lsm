package ru.mail.polis.test.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.nikitazadorotskas.PersistentDao;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 4)
public class MemorySegmentDaoFactory implements DaoFactory.Factory<MemorySegment, BaseEntry<MemorySegment>> {
    @Override
    public Dao<MemorySegment, BaseEntry<MemorySegment>> createDao() throws IOException {
        return new PersistentDao(null);
    }

    @Override
    public Dao<MemorySegment, BaseEntry<MemorySegment>> createDao(Config config) throws IOException {
        return new PersistentDao(config);
    }

    @Override
    public String toString(MemorySegment data) {
        return data == null ? null : new String(data.toByteArray(), StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        return data == null ? null : MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public BaseEntry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return new BaseEntry<>(baseEntry.key(), baseEntry.value());
    }
}
