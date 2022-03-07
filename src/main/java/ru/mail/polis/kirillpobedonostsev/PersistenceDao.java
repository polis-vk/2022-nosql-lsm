package ru.mail.polis.kirillpobedonostsev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistenceDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final int MAX_ENTRIES = 1_000_000;
    private static final String DATA_FILENAME = "data.txt";
    private static final String INDEX_FILENAME = "index.txt";
    private final Path dataPath;
    private final Path indexPath;
    private final MemorySegmentWriter writer;
    private final FileSeeker seeker;
    private final ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    private final ConcurrentMap<MemorySegment, Long> index =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    public PersistenceDao(Config config) {
        dataPath = config.basePath().resolve(DATA_FILENAME);
        indexPath = config.basePath().resolve(INDEX_FILENAME);
        try {
            if (!Files.exists(dataPath)) {
                Files.createFile(dataPath);
            }
            if (!Files.exists(indexPath)) {
                Files.createFile(indexPath);
            }
        } catch (IOException ignored) {
        }
        writer = new MemorySegmentWriter(dataPath, indexPath);
        seeker = new FileSeeker(dataPath, indexPath);
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) throws IOException {
        if (from == null && to == null) {
            return map.values().iterator();
        } else if (from == null) {
            return map.headMap(to, false).values().iterator();
        } else if (to == null) {
            return map.tailMap(from, true).values().iterator();
        }
        return map.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) throws IOException {
        if (index.isEmpty()) {
            seeker.fill(index);
        }
        BaseEntry<MemorySegment> result = map.get(key);
        return result == null ? seeker.get(key, index.get(key)) : result;
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        map.put(entry.key(), entry);
        if (map.size() == MAX_ENTRIES) {
            try {
                flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        long readBytes = Files.size(dataPath);
        for (BaseEntry<MemorySegment> entry : map.values()) {
            writer.write(entry);
            index.put(entry.key(), readBytes);
            readBytes = readBytes + Long.BYTES * 2 + entry.key().byteSize() + entry.value().byteSize();
        }
        writer.write(index);
        map.clear();
    }
}
