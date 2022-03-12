package ru.mail.polis.medvedevalexey;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Logger;

import static ru.mail.polis.medvedevalexey.BinarySearchUtils.binarySearch;
import static ru.mail.polis.medvedevalexey.BinarySearchUtils.readFromChannel;

public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {

    private static final Logger LOGGER = Logger.getLogger(InMemoryDao.class.getName());

    private static final int FLUSH_THRESHOLD = 100_000;
    private static final String SUFFIX = ".dat";
    static final int NUM_OF_ROWS_SIZE = Integer.BYTES;
    static final int KEY_LENGTH_SIZE = Integer.BYTES;
    static final int VALUE_LENGTH_SIZE = Integer.BYTES;
    static final int OFFSET_VALUE_SIZE = Long.BYTES;

    private final Path path;
    private int generation;

    private final ConcurrentNavigableMap<byte[], BaseEntry<byte[]>> storage =
            new ConcurrentSkipListMap<>(Arrays::compare);

    public InMemoryDao() {
        this.path = null;
    }

    public InMemoryDao(Config config) {
        this.path = config.basePath();
        File[] files = path.toFile().listFiles();
        generation = files == null ? 0 : files.length;
    }

    @Override
    public Iterator<BaseEntry<byte[]>> get(byte[] from, byte[] to) {
        if (from == null && to == null) {
            return storage.values().iterator();
        }
        if (from == null) {
            return storage.headMap(to).values().iterator();
        }
        if (to == null) {
            return storage.tailMap(from).values().iterator();
        }
        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public BaseEntry<byte[]> get(byte[] key) throws IOException {
        BaseEntry<byte[]> entry = storage.get(key);
        return entry == null ? getFromFile(key) : entry;
    }

    @Override
    public void upsert(BaseEntry<byte[]> entry) {
        if (entry == null) {
            throw new IllegalArgumentException();
        }
        storage.put(entry.key(), entry);

        if (storage.size() > FLUSH_THRESHOLD) {
            try {
                flush();
            } catch (IOException e) {
                LOGGER.throwing(InMemoryDao.class.getName(), "upsert", e);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        Path newFilePath = this.path.resolve(generation + SUFFIX);

        try (FileChannel channel = FileChannel.open(newFilePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            int numOfRows = storage.values().size();
            // смещения (начала) строк в файле (используется в бинарном поиске)
            ByteBuffer offsets = ByteBuffer.allocate(numOfRows * OFFSET_VALUE_SIZE);

            for (BaseEntry<byte[]> entry : storage.values()) {
                offsets.putLong(channel.position());
                channel.write(toByteBuffer(entry.key().length, KEY_LENGTH_SIZE));
                channel.write(toByteBuffer(entry.key()));
                channel.write(toByteBuffer(entry.value().length, VALUE_LENGTH_SIZE));
                channel.write(toByteBuffer(entry.value()));
            }

            channel.write(offsets.rewind());
            channel.write(toByteBuffer(numOfRows, NUM_OF_ROWS_SIZE));
        }

        generation++;
        storage.clear();
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private BaseEntry<byte[]> getFromFile(byte[] requiredKey) throws IOException {
        ByteBuffer requiredKeyByteBuffer = toByteBuffer(requiredKey);
        Path filePath;

        for (int i = generation - 1; i >= 0; i--) {
            filePath = this.path.resolve(i + SUFFIX);

            try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
                int numOfRows = readFromChannel(channel, NUM_OF_ROWS_SIZE, channel.size() - NUM_OF_ROWS_SIZE)
                        .rewind()
                        .getInt();

                ByteBuffer value = binarySearch(channel, numOfRows, requiredKeyByteBuffer);

                if (value != null) {
                    return new BaseEntry<>(requiredKey, value.array());
                }
            }
        }

        return null;
    }

    private ByteBuffer toByteBuffer(byte[] value) {
        return ByteBuffer.allocate(value.length).put(value).rewind();
    }

    private ByteBuffer toByteBuffer(int value, int size) {
        return ByteBuffer.allocate(size).putInt(value).rewind();
    }
}
