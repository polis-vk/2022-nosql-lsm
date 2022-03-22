package ru.mail.polis.test.dmitrykondraev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.dmitrykondraev.FileBackedDao;
import ru.mail.polis.test.DaoFactory;

/**
 * Author: Dmitry Kondraev.
 */

@DaoFactory(stage = 3)
public class MemorySegmentDaoFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(Config config) {
        return new FileBackedDao(config);
    }

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new FileBackedDao();
    }

    @Override
    public String toString(MemorySegment data) {
        return data == null ? null : new String(data.toCharArray());
    }

    @Override
    public MemorySegment fromString(String data) {
        // MemorySegment backed by heap
        return data == null ? null : MemorySegment.ofArray(data.toCharArray());
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
