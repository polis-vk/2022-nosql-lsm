package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentNavigableMap;

public class DaoWriter implements Closeable {
    private final FileOutputStream writer;
    private final FileOutputStream indexWriter;

    public DaoWriter(Path fileName, Path indexName) throws FileNotFoundException {
        writer = new FileOutputStream(String.valueOf(fileName));
        indexWriter = new FileOutputStream(String.valueOf(indexName));
    }

    public void writeMap(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map) throws IOException {
        writeInt(map.size());
        for (BaseEntry<ByteBuffer> entry : map.values()) {
            writeElementPair(entry);
        }
    }

    public void writeElementPair(BaseEntry<ByteBuffer> entry) throws IOException {
        writeLong(writer.getChannel().position());
        int keyLen = entry.key().array().length;
        int valLen = entry.value().array().length;
        ByteBuffer buffer = ByteBuffer.wrap(new byte[2 * Integer.BYTES + keyLen + valLen]);
        buffer.putInt(keyLen);
        buffer.putInt(valLen);
        buffer.put(entry.key().array());
        buffer.put(entry.value().array());
        writer.write(buffer.array());
    }

    public void writeLong(long size) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[Long.BYTES]);
        buffer.putLong(size);
        indexWriter.write(buffer.array());
    }

    public void writeInt(int size) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[Integer.BYTES]);
        buffer.putInt(size);
        indexWriter.write(buffer.array());
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
