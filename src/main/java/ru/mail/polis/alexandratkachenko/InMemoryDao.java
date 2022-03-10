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

    public InMemoryDao(Config config) throws IOException {
        Objects.requireNonNull(config, "Invalid argument in constructor.\n");
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
                    ByteBuffer value = getByteBufferBaseEntry(keySearch, channel);
                    if (value != null) {
                        return new BaseEntry<>(keySearch, value);
                    }
                }
            }
        }
        return null;
    }

    private ByteBuffer getByteBufferBaseEntry(ByteBuffer keySearch, FileChannel fileChannel) throws IOException {
        ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
        if (fileChannel.read(size) > 0) {
            ByteBuffer key = readComponent(fileChannel, size);
            if (size.getInt() >= 0 && keySearch.equals(key)) {
                return readComponent(fileChannel, size);
            }
        }
        return null;
    }

    private ByteBuffer readComponent(FileChannel fileChannel, ByteBuffer size) throws IOException {
        extracted(fileChannel, size);
        ByteBuffer value = ByteBuffer.allocate(size.getInt());
        extracted(fileChannel, value);
        return value;
    }

    private void extracted(FileChannel fileChannel, ByteBuffer value) throws IOException {
        value.position(0);
        while (value.position() != value.capacity()) {
            fileChannel.read(value);
        }
        value.position(0);
    }

    @Override
    public void flush() throws IOException {
        write();
        map.clear();
    }

    private void write() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(dataPath, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)) {
            ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (BaseEntry<ByteBuffer> iterator : map.values()) {
                writeComponent(iterator.key(), fileChannel, size);
                writeComponent(iterator.value(), fileChannel, size);
            }
        }
    }

    private void writeComponent(ByteBuffer value, FileChannel channel, ByteBuffer buffer) throws IOException {
        buffer.rewind();
        buffer.putInt(value.remaining());
        buffer.rewind();
        channel.write(buffer);
        channel.write(value);
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

