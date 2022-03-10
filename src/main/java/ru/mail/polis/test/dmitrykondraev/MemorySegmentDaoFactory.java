package ru.mail.polis.test.dmitrykondraev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.dmitrykondraev.FileBackedDao;
import ru.mail.polis.test.DaoFactory;

import java.nio.charset.StandardCharsets;

/**
 * Author: Dmitry Kondraev.
 */

@DaoFactory(stage = 2)
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
        return new String(data.toByteArray(), StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        if (data == null) {
            return null;
        }
        // MemorySegment backed by heap
        return MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}