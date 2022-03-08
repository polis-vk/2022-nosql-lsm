package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.SortedMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class FileManager {
    private static final String DATA_UNIT_NAME = "storage";
    private static final String EXTENSION = ".txt";
    private static final int BUFFER_SIZE = 100;

    private final Path pathToWrite;
    Config config;

    public FileManager(Config config) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException();
        }
        this.config = config;
        pathToWrite = config.basePath().resolve(DATA_UNIT_NAME + EXTENSION);
        File sourceDirectory = config.basePath().toFile();
        if (!sourceDirectory.exists() || !sourceDirectory.isDirectory()) {
            boolean isDirCreated = config.basePath().toFile().mkdir();
            if (!isDirCreated) {
                throw new IOException();
            }
        }
    }

    public BaseEntry<ByteBuffer> getByKey(ByteBuffer key) throws IOException {
        return searchFile(pathToWrite.toFile(), key);
    }

    private BaseEntry<ByteBuffer> searchFile(File file, ByteBuffer key) throws IOException {
        if ((file == null) || (key == null)) {
            throw new IllegalArgumentException();
        }
        BaseEntry<ByteBuffer> result = null;
        try (RandomAccessFile fileToRead = new RandomAccessFile(file, "rw");
             FileChannel ch = fileToRead.getChannel()) {
            while (ch.position() < ch.size()) {
                int keySize = fileToRead.readInt();
                ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
                ch.read(keyBuffer);
                keyBuffer.rewind();
                int valueSize = fileToRead.readInt();
                if (keyBuffer.equals(key)) {
                    ByteBuffer valueBuffer = ByteBuffer.allocate(valueSize);
                    ch.read(valueBuffer);
                    valueBuffer.rewind();
                    result = new BaseEntry<ByteBuffer>(key, valueBuffer);
                } else {
                    fileToRead.skipBytes(valueSize);
                }
            }
            return result;
        }
    }

    public void store(SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> data) throws IOException {
        if (data == null) {
            throw new IllegalArgumentException();
        }
        try (FileChannel channel = new RandomAccessFile(pathToWrite.toFile(), "rw").getChannel()) {
            CopyOnWriteArrayList<ByteBuffer> entryBuffer = new CopyOnWriteArrayList<ByteBuffer>();
            int size = 0;
            for (BaseEntry<ByteBuffer> e : data.values()) {
                ByteBuffer entry = getBufferFromEntry(e);
                entryBuffer.add(entry);
                size += entry.remaining();
                if (entryBuffer.size() == BUFFER_SIZE) {
                    channel.write(getBufferFromList(entryBuffer, size));
                    entryBuffer.clear();
                    size = 0;
                }
            }
            if (!entryBuffer.isEmpty()) {
                channel.write(getBufferFromList(entryBuffer, size));
            }
            channel.force(false);
        }
    }

    private ByteBuffer getBufferFromEntry(BaseEntry<ByteBuffer> e) {
        final int sizeToAllocate = e.key().remaining() + e.value().remaining() + 2 * Integer.BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(sizeToAllocate);
        buffer.putInt(e.key().remaining());
        buffer.put(e.key());
        buffer.putInt(e.value().remaining());
        buffer.put(e.value());
        buffer.rewind();
        return buffer;
    }

    private ByteBuffer getBufferFromList(CopyOnWriteArrayList<ByteBuffer> buffer, int size) {
        ByteBuffer result = ByteBuffer.allocate(size);
        for (ByteBuffer el : buffer) {
            result.put(el);
        }
        result.rewind();
        return result;
    }
}
