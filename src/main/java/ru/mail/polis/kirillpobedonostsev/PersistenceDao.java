package ru.mail.polis.kirillpobedonostsev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistenceDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private static final String DATA_FILENAME = "data.txt";
    private final FileWriter writer;
    private final FileSeeker seeker;
    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map =
            new ConcurrentSkipListMap<>(ByteBuffer::compareTo);

    public PersistenceDao(Config config) {
        Path dataPath = config.basePath().resolve(DATA_FILENAME);
        try {
            if (!Files.exists(dataPath)) {
                Files.createDirectories(config.basePath());
                Files.createFile(dataPath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        writer = new FileWriter(dataPath);
        seeker = new FileSeeker(dataPath);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
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
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> result = map.get(key);
        return result == null ? seeker.tryFind(key) : result;
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        map.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        writer.write(map);
        map.clear();
    }
}
