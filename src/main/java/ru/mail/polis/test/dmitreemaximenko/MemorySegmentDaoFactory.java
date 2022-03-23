package ru.mail.polis.test.dmitreemaximenko;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.dmitreemaximenko.MemorySegmentInMemoryDao;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;

@DaoFactory(stage = 3, week = 5)
public class MemorySegmentDaoFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(Config config) throws IOException {
        return new MemorySegmentInMemoryDao(config);
    }

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() throws IOException {
        return new MemorySegmentInMemoryDao();
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
