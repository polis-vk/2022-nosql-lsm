package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class MapInputStream extends FileInputStream {

    public MapInputStream(File file) throws FileNotFoundException {
        super(file);
    }

    public BaseEntry<ByteBuffer> readByKey(ByteBuffer key) throws IOException {
        ByteBuffer currKey = readByteBuffer();
        while (available() > 0 && currKey.compareTo(key) < 0) {
            skipValue();
            currKey = readByteBuffer();
        }
        if (currKey.compareTo(key) == 0) {
            return new BaseEntry<>(currKey, readByteBuffer());
        }
        return null;
    }

    private ByteBuffer readByteBuffer() throws IOException {
        int length = readInt();
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

    private void skipValue() throws IOException {
        int length = readInt();
        if (skip(length) != length) {
            throw new EOFException("Skip value error");
        }
    }

}
