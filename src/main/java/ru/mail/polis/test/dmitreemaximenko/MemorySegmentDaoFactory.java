package ru.mail.polis.test.dmitreemaximenko;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.dmitreemaximenko.MemorySegmentDao;
import ru.mail.polis.test.DaoFactory;
import java.io.IOException;

@DaoFactory(stage = 4, week = 1)
public class MemorySegmentDaoFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(Config config) throws IOException {
        return new MemorySegmentDao(config);
    }

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() throws IOException {
        return new MemorySegmentDao();
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        return memorySegment == null ? null : new String(memorySegment.toCharArray());
    }

    @Override
    public MemorySegment fromString(String string) {
        return string == null ? null : MemorySegment.ofArray(string.toCharArray());
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> entry) {
        return entry;
    }
}
