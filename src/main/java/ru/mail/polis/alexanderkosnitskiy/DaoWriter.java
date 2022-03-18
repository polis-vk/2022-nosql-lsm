package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentNavigableMap;

public class DaoWriter implements Closeable {
    private final FileChannel writer;
    private final FileChannel indexWriter;

    public DaoWriter(Path fileName, Path indexName) throws IOException {
        writer = FileChannel.open(fileName,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
        indexWriter = FileChannel.open(indexName,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    public void writeMap(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map) throws IOException {
        MappedByteBuffer indexMapper = indexWriter.map
                (FileChannel.MapMode.READ_WRITE, 0, (long) Integer.BYTES * (map.size() + 1));
        indexMapper.putInt(map.size());
        int size = 0;
        for (BaseEntry<ByteBuffer> entry : map.values()) {
            indexMapper.putInt(size);
            int capacity = entry.value() != null ? entry.value().capacity() : 0;
            size += 2 * Integer.BYTES + entry.key().capacity() + capacity;
        }

        MappedByteBuffer mapper = writer.map(FileChannel.MapMode.READ_WRITE, 0, size);

        for (BaseEntry<ByteBuffer> entry : map.values()) {
            mapper.putInt(entry.key().capacity());
            int capacity = entry.value() != null ? entry.value().capacity() : -1;
            mapper.putInt(capacity);
            mapper.put(entry.key());
            if (entry.value() != null) {
                mapper.put(entry.value());
            }
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
        indexWriter.close();
    }
}
