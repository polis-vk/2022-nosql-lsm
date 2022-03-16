package ru.mail.polis.alexandratkachenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private static final String DATA_FILENAME = "dao_data.txt";
    private final Path dataPath;

    private final ConcurrentSkipListMap<ByteBuffer, BaseEntry<ByteBuffer>> map = new ConcurrentSkipListMap<>();

    public InMemoryDao(Config config) {
        Objects.requireNonNull(config, "Invalid argument - config.\n");
        dataPath = config.basePath().resolve(DATA_FILENAME);
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        Objects.requireNonNull(key, "Invalid argument in get().\n");
        BaseEntry<ByteBuffer> value = map.get(key);
        return (value == null) ? search(key) : value;
    }

    private BaseEntry<ByteBuffer> search(ByteBuffer keySearch) throws IOException {
        if (Files.exists(dataPath)) {
            try (FileChannel channel = FileChannel.open(dataPath)) {
                while (true) {
                    ByteBuffer key = readComponent(channel);
                    if (key == null) {
                        return null;
                    }
                    ByteBuffer value = readComponent(channel);
                    if (value == null) {
                        return null;
                    }
                    if (keySearch.equals(key)) {
                        return new BaseEntry<>(keySearch, value);
                    }
                }
            }
        }
        return null;
    }

    @Override
    public void flush() throws IOException {
        write();
        map.clear();
    }

    private void write() throws IOException {
        try (FileChannel channel = FileChannel.open(dataPath, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)) {
            for (BaseEntry<ByteBuffer> iterator : map.values()) {
                writeComponent(iterator.key(), channel);
                writeComponent(iterator.value(), channel);
            }
        }
    }

    private ByteBuffer readComponent(FileChannel fileChannel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        if (fileChannel.read(buffer) != Integer.BYTES) {
            return null;
        }
        buffer.flip();
        int size = buffer.getInt();
        ByteBuffer component = ByteBuffer.allocate(size);
        fileChannel.read(component);
        component.flip();
        return component;
    }

    private void writeComponent(ByteBuffer component, FileChannel channel) throws IOException {
        ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
        size.putInt(component.remaining());
        size.flip();
        channel.write(size);
        channel.write(component);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (map.isEmpty()) {
            return Collections.emptyIterator();
        }
        ConcurrentMap<ByteBuffer, BaseEntry<ByteBuffer>> result;
        if (from == null && to == null) {
            result = map;
        } else if (from == null) {
            result = map.headMap(to);
        } else if (to == null) {
            result = map.tailMap(from);
        } else {
            result = map.subMap(from, to);
        }
        return result.values().iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        Objects.requireNonNull(entry, "Invalid argument in upsert().\n");
        map.put(entry.key(), entry);
    }
}

