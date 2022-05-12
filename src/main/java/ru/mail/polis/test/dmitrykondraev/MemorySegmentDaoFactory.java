package ru.mail.polis.test.dmitrykondraev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.dmitrykondraev.ConcurrentFilesBackedDao;
import ru.mail.polis.dmitrykondraev.MemorySegmentEntry;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;

/**
 * Author: Dmitry Kondraev.
 */

@DaoFactory(stage = 5)
public class MemorySegmentDaoFactory implements DaoFactory.Factory<MemorySegment, MemorySegmentEntry> {

    @Override
    public Dao<MemorySegment, MemorySegmentEntry> createDao(Config config) throws IOException {
        return ConcurrentFilesBackedDao.of(config);
    }

    @Override
    public Dao<MemorySegment, MemorySegmentEntry> createDao() {
        throw new UnsupportedOperationException("Can't create Dao without config");
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
    public MemorySegmentEntry fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return MemorySegmentEntry.of(baseEntry.key(), baseEntry.value());
    }
}
