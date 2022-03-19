package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

public class FileManager {
    private static final String DATA_UNIT_NAME = "storage";
    private static final String EXTENSION = ".txt";
    private static final int BUFFER_SIZE = 100;

    private final Path pathToWrite;
    Config config;

    public FileManager(Config config) {
        if (config == null) {
            throw new IllegalArgumentException();
        }
        this.config = config;
        pathToWrite = config.basePath().resolve(DATA_UNIT_NAME + EXTENSION);
    }

    public BaseEntry<ByteBuffer> getByKey(ByteBuffer key) throws IOException {
        if (Files.notExists(pathToWrite)) {
            return null;
        }
        BaseEntry<ByteBuffer> result = null;
        try (RandomAccessFile fileToRead = new RandomAccessFile(pathToWrite.toFile(), "rw");
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
            return;
        }
        try (FileChannel channel = new RandomAccessFile(pathToWrite.toFile(), "rw").getChannel()) {
            ArrayList<ByteBuffer> entryBuffer = new ArrayList<ByteBuffer>();
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

    private ByteBuffer getBufferFromList(List<ByteBuffer> buffer, int size) {
        ByteBuffer result = ByteBuffer.allocate(size);
        for (ByteBuffer el : buffer) {
            result.put(el);
        }
        result.rewind();
        return result;
    }
}
