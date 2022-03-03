package ru.mail.polis.test.glebkomissarov;

import jdk.incubator.foreign.MemorySegment;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.glebkomissarov.MyMemoryDao;
import ru.mail.polis.test.DaoFactory;

@DaoFactory(stage = 1, week = 2)
public class MemorySegmentDaoFactory implements DaoFactory.Factory<MemorySegment, BaseEntry<MemorySegment>> {
    @Override
    public Dao<MemorySegment, BaseEntry<MemorySegment>> createDao() {
        return new MyMemoryDao();
    }

    @Override
    public String toString(@Nullable MemorySegment data) {
        return data == null ? null : new String(data.toCharArray());
    }

    @Override
    public MemorySegment fromString(@Nullable String data) {
        return data == null ? null : MemorySegment.ofArray(data.toCharArray());
    }

    @Override
    public BaseEntry<MemorySegment> fromBaseEntry(@NotNull Entry<MemorySegment> baseEntry) {
        return new BaseEntry<>(baseEntry.key(), baseEntry.value());
    }
}
