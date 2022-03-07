package ru.mail.polis.test.kirillpobedonostsev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.kirillpobedonostsev.PersistenceDao;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 2, week = 1)
public class MemorySegmentDaoFactory implements DaoFactory.Factory<MemorySegment, BaseEntry<MemorySegment>> {
    @Override
    public Dao<MemorySegment, BaseEntry<MemorySegment>> createDao(Config config) throws IOException {
        return new PersistenceDao(config);
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
