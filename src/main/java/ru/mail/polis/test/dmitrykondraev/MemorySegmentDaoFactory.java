package ru.mail.polis.test.dmitrykondraev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.dmitrykondraev.FileBackedDao;
import ru.mail.polis.test.DaoFactory;

/**
 * Author: Dmitry Kondraev.
 */

@DaoFactory(stage = 2)
public class MemorySegmentDaoFactory implements DaoFactory.Factory<MemorySegment, BaseEntry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, BaseEntry<MemorySegment>> createDao(Config config) {
        return new FileBackedDao(config);
    }

    @Override
    public Dao<MemorySegment, BaseEntry<MemorySegment>> createDao() {
        return new FileBackedDao();
    }

    @Override
    public String toString(MemorySegment data) {
        return new String(data.toCharArray());
    }

    @Override
    public MemorySegment fromString(String data) {
        if (data == null) {
            return null;
        }
        // MemorySegment backed by heap
        return MemorySegment.ofArray(data.toCharArray());
    }

    @Override
    public BaseEntry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return new BaseEntry<>(baseEntry.key(), baseEntry.value());
    }
}
