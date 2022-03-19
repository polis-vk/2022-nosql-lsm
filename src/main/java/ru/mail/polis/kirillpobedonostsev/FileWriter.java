package ru.mail.polis.kirillpobedonostsev;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentNavigableMap;

public class FileWriter {
    private final Path dataPath;
    private final Path indexPath;

    public FileWriter(Path dataPath, Path indexPath) {
        this.dataPath = dataPath;
        this.indexPath = indexPath;
    }

    public void write(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map) throws IOException {
        long offset = 0;
        ByteBuffer indexResult = ByteBuffer.allocate(map.size() * Long.BYTES);
        try (FileChannel channel = new RandomAccessFile(dataPath.toFile(), "rw").getChannel()) {
            for (BaseEntry<ByteBuffer> entry : map.values()) {
                int entryLength = entry.key().remaining() + entry.value().remaining() + Integer.BYTES * 2;
                ByteBuffer result = ByteBuffer.allocate(entryLength);
                result.putInt(entry.key().remaining());
                result.put(entry.key());
                result.putInt(entry.value().remaining());
                result.put(entry.value());
                result.rewind();
                channel.write(result);
                indexResult.putLong(offset);
                offset += entryLength;
            }
        }
        indexResult.rewind();
        try (FileChannel channel = new RandomAccessFile(indexPath.toFile(), "rw").getChannel()) {
            channel.write(indexResult);
        }
    }
}
