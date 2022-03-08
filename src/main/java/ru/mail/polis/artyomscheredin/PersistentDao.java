package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;


public class PersistentDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private static final int MAX_CAPACITY = 100000;

    private final SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> inMemoryData =
            new ConcurrentSkipListMap<>(ByteBuffer::compareTo);
    private final FileManager fileManager;

    public PersistentDao(Config config) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException();
        }
        fileManager = new FileManager(config);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (inMemoryData.isEmpty()) {
            return Collections.emptyIterator();
        }

        if ((from == null) && (to == null)) {
            return inMemoryData.values().iterator();
        } else if (from == null) {
            return inMemoryData.headMap(to).values().iterator();
        } else if (to == null) {
            return inMemoryData.tailMap(from).values().iterator();
        }
        return inMemoryData.subMap(from, to).values().iterator();
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException();
        }
        BaseEntry<ByteBuffer> value = inMemoryData.get(key);
        if (value == null) {
            value = fileManager.getByKey(key);
        }
        return value;
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        if (entry == null) {
            throw new IllegalArgumentException();
        }
        if (inMemoryData.size() == MAX_CAPACITY) {
            try {
                flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        inMemoryData.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        fileManager.store(inMemoryData);
        inMemoryData.clear();
    }

    @Override
    public void close() throws IOException {
        this.flush();
    }
}
