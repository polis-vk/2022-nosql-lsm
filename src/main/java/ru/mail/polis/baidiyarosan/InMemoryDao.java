package ru.mail.polis.baidiyarosan;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private static final String DATA_FILE_NAME = "data.log";

    private final NavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> collection = new ConcurrentSkipListMap<>();

    private final Path path;

    public InMemoryDao(Config config) throws IOException {
        this.path = config.basePath().resolve(DATA_FILE_NAME);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        if (collection.isEmpty()) {
            return Collections.emptyIterator();
        }

        if (from == null && to == null) {
            return collection.values().iterator();
        }

        return collection.subMap(
                from == null ? collection.firstKey() : from, true,
                to == null ? collection.lastKey() : to, to == null
        ).values().iterator();

    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        collection.put(entry.key(), entry);
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {

        BaseEntry<ByteBuffer> value = collection.get(key);
        if (value != null) {
            return value;
        }
        if (!Files.exists(path)) {
            return null;
        }
        return searchFile(key);
    }

    private BaseEntry<ByteBuffer> searchFile(ByteBuffer key) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
             FileChannel in = raf.getChannel()) {
            long size = 0L;
            ByteBuffer temp = ByteBuffer.allocate(Integer.BYTES);
            while (size < in.size()) {
                int keySize = readInt(in, temp);
                size += Integer.BYTES;
                if (key.capacity() != keySize) {
                    continue;
                }
                ByteBuffer compareKey = readBuffer(in, keySize);
                size += compareKey.capacity();
                if (key.equals(compareKey)) {
                    return new BaseEntry<>(key, readBuffer(in, readInt(in, temp)));
                }
            }
        }
        return null;
    }

    private int readInt(FileChannel in, ByteBuffer temp) throws IOException {
        temp.clear();
        in.read(temp);
        temp.flip();
        return temp.getInt();
    }

    private ByteBuffer readBuffer(FileChannel in, int size) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        in.read(buffer);
        return buffer.flip();
    }

    @Override
    public void flush() throws IOException {
        if (collection.isEmpty()) {
            return;
        }
        int size;
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{});
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
             FileChannel out = raf.getChannel()) {
            for (BaseEntry<ByteBuffer> entry : collection.values()) {
                size = sizeOfEntry(entry);
                if (buffer.capacity() != size) {
                    buffer = ByteBuffer.allocate(size);
                }
                buffer.putInt(entry.key().capacity()).put(entry.key());
                buffer.putInt(entry.value().capacity()).put(entry.value());
                buffer.flip();
                out.write(buffer);
                buffer.clear();
            }
        }
    }

    private int sizeOfEntry(BaseEntry<ByteBuffer> entry) {
        return 2 * Integer.BYTES + entry.key().capacity() + entry.value().capacity();
    }
}
