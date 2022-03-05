package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MapInputStream extends FileInputStream {

    public MapInputStream(File file) throws FileNotFoundException {
        super(file);
    }

    public SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> readMap() throws IOException {
        SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> data = new ConcurrentSkipListMap<>();
        while (available() > 0) {
            ByteBuffer key = readByteBuffer();
            BaseEntry<ByteBuffer> value = readBaseEntry();
            data.put(key, value);
        }
        return data;
    }

    private BaseEntry<ByteBuffer> readBaseEntry() throws IOException {
        ByteBuffer baseKey = readByteBuffer();
        ByteBuffer baseValue = readByteBuffer();
        return new BaseEntry<>(baseKey, baseValue);
    }

    private ByteBuffer readByteBuffer() throws IOException {
        int length = readInt();
        if (length == -1) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.allocate(length);
        byte[] bytes = new byte[length];
        if (read(bytes) != length) {
            throw new EOFException("Error parse string");
        }
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }

    private Integer readInt() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        byte[] bytes = new byte[Integer.BYTES];
        if (read(bytes) != Integer.BYTES) {
            throw new EOFException("Error parse int");
        }
        buffer.put(bytes);
        buffer.flip();
        return buffer.getInt();
    }

}
