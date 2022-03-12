package ru.mail.polis.medvedevalexey;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Logger;


public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {

    private static final Logger LOGGER = Logger.getLogger(InMemoryDao.class.getName());

    private static final int FLUSH_THRESHOLD = 100_000;
    private static final String SUFFIX = ".dat";

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

        if (!Files.exists(newFilePath)) {
            Files.createFile(newFilePath);
        }

        try (FileChannel channel = FileChannel.open(newFilePath, StandardOpenOption.WRITE)) {
            for (BaseEntry<byte[]> entry : storage.values()) {
                channel.write(ByteBuffer.allocate(Integer.BYTES).putInt(entry.key().length).rewind());
                channel.write(ByteBuffer.allocate(entry.key().length).put(entry.key()).rewind());
                channel.write(ByteBuffer.allocate(Integer.BYTES).putInt(entry.value().length).rewind());
                channel.write(ByteBuffer.allocate(entry.value().length).put(entry.value()).rewind());
            }
            channel.write(ByteBuffer.allocate(Integer.BYTES).putInt(storage.values().size()).rewind());
        }

        generation++;
        storage.clear();
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private BaseEntry<byte[]> getFromFile(byte[] requiredKey) throws IOException {
        ByteBuffer buffer;
        int valueLength;
        int keyLength;
        Path filePath;
        byte[] value;
        byte[] key;

        for (int i = generation - 1; i >= 0; i--) {
            filePath = this.path.resolve(i + SUFFIX);

            try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
                buffer = ByteBuffer.allocate(Integer.BYTES);
                channel.read(buffer, channel.size() - Integer.BYTES);
                int numOfRows = buffer.rewind().getInt();

                for (int j = 0; j < numOfRows; j++) {
                    buffer = ByteBuffer.allocate(Integer.BYTES);
                    channel.read(buffer);
                    keyLength = buffer.rewind().getInt();

                    buffer = ByteBuffer.allocate(keyLength);
                    channel.read(buffer);
                    key = buffer.array();

                    buffer = ByteBuffer.allocate(Integer.BYTES);
                    channel.read(buffer);
                    valueLength = buffer.rewind().getInt();

                    if (Arrays.compare(requiredKey, key) == 0) {
                        buffer = ByteBuffer.allocate(valueLength);
                        channel.read(buffer);
                        value = buffer.array();

                        return new BaseEntry<>(key, value);
                    }

                    channel.position(channel.position() + valueLength);
                }
            }
        }

        return null;
    }
}
