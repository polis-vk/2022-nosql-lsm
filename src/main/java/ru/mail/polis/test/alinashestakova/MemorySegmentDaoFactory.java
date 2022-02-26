package ru.mail.polis.test.alinashestakova;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.alinashestakova.InMemoryDao;
import ru.mail.polis.test.DaoFactory;

import java.nio.charset.StandardCharsets;

@DaoFactory
public class MemorySegmentDaoFactory implements DaoFactory.Factory<MemorySegment, BaseEntry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, BaseEntry<MemorySegment>> createDao() {
        return new InMemoryDao();
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
        return baseEntry == null ? null : new BaseEntry<>(baseEntry.key(), baseEntry.value());
    }
}
