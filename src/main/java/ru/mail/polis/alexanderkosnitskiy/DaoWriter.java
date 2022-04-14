package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
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
        MappedByteBuffer indexMapper = indexWriter.map(FileChannel.MapMode.READ_WRITE, 0,
                (long) Integer.BYTES * (map.size() + 1));
        indexMapper.putInt(map.size());
        int size = 0;
        for (BaseEntry<ByteBuffer> entry : map.values()) {
            indexMapper.putInt(size);
            int capacity = entry.value() == null ? 0 : entry.value().capacity();
            size += 2 * Integer.BYTES + entry.key().capacity() + capacity;
        }

        MappedByteBuffer mapper = writer.map(FileChannel.MapMode.READ_WRITE, 0, size);

        for (BaseEntry<ByteBuffer> entry : map.values()) {
            mapper.putInt(entry.key().capacity());
            int capacity = entry.value() == null ? -1 : entry.value().capacity();
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

    public void writeIterator(Iterator<BaseEntry<ByteBuffer>> iterator, int count, int size) throws IOException {
        MappedByteBuffer indexMapper = indexWriter.map(FileChannel.MapMode.READ_WRITE, 0,
                (long) Integer.BYTES * (count + 1));
        indexMapper.putInt(count);
        MappedByteBuffer mapper = writer.map(FileChannel.MapMode.READ_WRITE, 0, size);
        int position = 0;
        while (iterator.hasNext()) {
            BaseEntry<ByteBuffer> entry = iterator.next();
            indexMapper.putInt(position);
            position += 2 * Integer.BYTES + entry.key().capacity() + entry.value().capacity();
            mapper.putInt(entry.key().capacity());
            mapper.putInt(entry.value().capacity());
            mapper.put(entry.key());
            mapper.put(entry.value());
        }
    }
}
