package ru.mail.polis.test.glebkomissarov;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.glebkomissarov.MyMemoryDao;
import ru.mail.polis.test.DaoFactory;

@DaoFactory
public class MemorySegmentDaoFactory implements DaoFactory.Factory<MemorySegment, BaseEntry<MemorySegment>> {
    @Override
    public Dao<MemorySegment, BaseEntry<MemorySegment>> createDao() {
        return new MyMemoryDao();
    }

    @Override
    public String toString(@Nullable MemorySegment data) {
        return data == null ? null : new String(data.toByteArray(), Charset.defaultCharset());
    }

    @Override
    public MemorySegment fromString(@Nullable String data) {
        return data == null ? null : MemorySegment.ofArray(data.getBytes(Charset.defaultCharset()));
    }

    @Override
    public BaseEntry<MemorySegment> fromBaseEntry(@NotNull Entry<MemorySegment> baseEntry) {
        return new BaseEntry<>(baseEntry.key(), baseEntry.value());
    }
}
