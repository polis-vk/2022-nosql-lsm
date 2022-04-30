package ru.mail.polis.stepanponomarev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Dao;
import ru.mail.polis.stepanponomarev.store.Storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class LSMDao implements Dao<MemorySegment, TimestampEntry> {
    private final Storage storage;

    public LSMDao(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Path: " + path + " is not exist");
        }

        storage = new Storage(path);
    }

    @Override
    public Iterator<TimestampEntry> get(MemorySegment from, MemorySegment to) throws IOException {
        return new TombstoneSkipIterator<>(storage.get(from, to));
    }

    @Override
    public TimestampEntry get(MemorySegment key) throws IOException {
        return storage.get(key);
    }

    @Override
    public void upsert(TimestampEntry entry) {
        storage.put(entry);
    }

    @Override
    public void close() throws IOException {
        flush();
        storage.close();
    }

    @Override
    public void compact() throws IOException {
        final long timestamp = System.currentTimeMillis();
        storage.compact(timestamp);
    }

    @Override
    public void flush() throws IOException {
        final long timestamp = System.currentTimeMillis();
        storage.flush(timestamp);
    }
}
