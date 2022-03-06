package ru.mail.polis.alexanderkosnitskiy;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentNavigableMap;

import ru.mail.polis.BaseEntry;

public class DaoWriter implements Closeable {
    private final FileOutputStream writer;

    public DaoWriter(String name) throws FileNotFoundException {
        writer = new FileOutputStream(name);
    }

    public void writeMap(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map) throws IOException {
        writeInt(map.size());
        for (BaseEntry<ByteBuffer> entry : map.values()) {
            writeElementPair(entry);
        }
    }

    public void writeElementPair(BaseEntry<ByteBuffer> entry) throws IOException {
        int keyLen = entry.key().array().length;
        int valLen = entry.value().array().length;
        ByteBuffer buffer = ByteBuffer.wrap(new byte[Integer.BYTES + Integer.BYTES + keyLen + valLen]);
        buffer.putInt(keyLen);
        buffer.putInt(valLen);
        buffer.put(entry.key().array());
        buffer.put(entry.value().array());
        writer.write(buffer.array());
    }

    public void writeElement(ByteBuffer element) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[Integer.BYTES + element.array().length]);
        buffer.putInt(element.array().length);
        buffer.put(element.array());
        writer.write(buffer.array());
    }

    public void writeInt(int size) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[Integer.BYTES]);
        buffer.putInt(size);
        writer.write(buffer.array());
    }

    public void write(ByteBuffer buffer) throws IOException {
        writer.write(buffer.array());
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
