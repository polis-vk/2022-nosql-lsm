package ru.mail.polis.artyomscheredin;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.SortedMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

public class FileManager {
    private static final String DATA_UNIT_NAME = "table";
    private static final String EXTENSION = ".txt";
    private static final int BUFFER_SIZE = 50;

    private Path pathToWrite;
    private int counter;
    Config config;

    public FileManager(Config config) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException();
        }
        this.config = config;
        File sourceDirectory = config.basePath().toFile();
        if (!sourceDirectory.exists() || !sourceDirectory.isDirectory()) {
            if (!config.basePath().toFile().mkdir()) {
                throw new IOException();
            }
        }
        counter = 0;
    }


    private void createDataFileIfNotExists() throws IOException {
        pathToWrite = config.basePath().resolve(DATA_UNIT_NAME + counter + EXTENSION);
        while (Files.exists(pathToWrite)) {
            counter++;
            pathToWrite = config.basePath().resolve(DATA_UNIT_NAME + counter + EXTENSION);
        }
        Files.createFile(pathToWrite);
    }


    public BaseEntry<ByteBuffer> getByKey(ByteBuffer key) throws IOException {
        Pattern pattern = Pattern.compile(DATA_UNIT_NAME + "[0-9]+" + EXTENSION);
        BaseEntry<ByteBuffer> result;

        File[] files = config.basePath().toFile().listFiles();
        for (int i = files.length - 1; i >= 0; i--) {
            Matcher matcher = pattern.matcher(files[i].getName());
            if (matcher.matches()) {
                result = searchFile(files[i], key);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
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
                keyBuffer.flip();
                int valueSize = fileToRead.readInt();
                if (keyBuffer.equals(key)) {
                    ByteBuffer valueBuffer = ByteBuffer.allocate(valueSize);
                    ch.read(valueBuffer);
                    result = new BaseEntry<ByteBuffer>(keyBuffer, valueBuffer);
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
        createDataFileIfNotExists();
        try (RandomAccessFile file = new RandomAccessFile(pathToWrite.toFile(), "rw");
             FileChannel channel = file.getChannel()) {
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
            channel.write(getBufferFromList(entryBuffer, size));
        }
        counter++;
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
