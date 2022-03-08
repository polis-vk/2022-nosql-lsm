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
        try (FileChannel channel = new RandomAccessFile(filePath.toFile(), "rw").getChannel()) {
            for (BaseEntry<ByteBuffer> entry : map.values()) {
                ByteBuffer result =
                        ByteBuffer.allocate(entry.key().remaining() + entry.value().remaining() + Integer.BYTES * 2);
                result.putInt(entry.key().remaining());
                result.put(entry.key());
                result.putInt(entry.value().remaining());
                result.put(entry.value());
                result.rewind();
                channel.write(result);
            }
        }
    }
}
