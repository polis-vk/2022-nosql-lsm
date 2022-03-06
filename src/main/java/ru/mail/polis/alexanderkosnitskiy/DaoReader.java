package ru.mail.polis.alexanderkosnitskiy;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import ru.mail.polis.BaseEntry;

public class DaoReader implements Closeable {
    private final FileInputStream reader;

    public DaoReader(String name) throws FileNotFoundException {
        reader = new FileInputStream(name);
    }

    public ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> readMap() throws IOException {
        int size = readInt();
        ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map = new ConcurrentSkipListMap<>();
        BaseEntry<ByteBuffer> entry;
        for(int i = 0; i < size; i++) {
            entry = readElementPair();
            map.put(entry.key(), entry);
        }
        return map;
    }

    public BaseEntry<ByteBuffer> readElementPair() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * 2);
        reader.read(buffer.array());
        buffer.rewind();
        int keyLen = buffer.getInt();
        int valLen = buffer.getInt();
        ByteBuffer keyBuffer = ByteBuffer.allocate(keyLen);
        ByteBuffer valBuffer = ByteBuffer.allocate(valLen);
        reader.read(keyBuffer.array());
        reader.read(valBuffer.array());
        keyBuffer.rewind();
        valBuffer.rewind();
        return new BaseEntry<ByteBuffer>(keyBuffer, valBuffer);
    }

    public ByteBuffer readElement() throws IOException {
        int size = readInt();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        reader.read(buffer.array());
        buffer.rewind();
        return buffer;
    }

    public int readInt() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        reader.read(buffer.array());
        buffer.rewind();
        return buffer.getInt();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
