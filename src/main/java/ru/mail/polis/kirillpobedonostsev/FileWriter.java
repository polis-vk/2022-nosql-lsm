package ru.mail.polis.kirillpobedonostsev;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentNavigableMap;

public class FileWriter {
    private final Path filePath;

    public FileWriter(Path filePath) {
        this.filePath = filePath;
    }

    public void write(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "rw");
             FileChannel channel = file.getChannel()) {
            file.seek(file.length());
            for (BaseEntry<ByteBuffer> entry : map.values()) {
                file.writeInt(entry.key().remaining());
                channel.write(entry.key());
                file.writeInt(entry.value().remaining());
                channel.write(entry.value());
            }
        }
    }
}
