package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class MapOutputStream extends FileOutputStream {

    private static final int BUFFER_SIZE = 1024;
    ByteBuffer localBuffer = ByteBuffer.allocate(BUFFER_SIZE);

    public MapOutputStream(String name) throws FileNotFoundException {
        super(name);
    }

    public void writeMap(Map<ByteBuffer, BaseEntry<ByteBuffer>> data) throws IOException {
        for (Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>> entry : data.entrySet()) {
            writeEntry(entry);
            byte[] array = new byte[localBuffer.position()];
            localBuffer.flip();
            localBuffer.get(array);
            write(array);
            localBuffer.clear();
        }
    }

    private void writeEntry(Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>> entry) {
        writeByteBuffer(entry.getKey());
        writeByteBuffer(entry.getValue().value());
    }

    private void writeByteBuffer(ByteBuffer buffer) {
        buffer.position(buffer.arrayOffset());
        byte[] array = buffer.array();
        writeInt(array.length);
        localBuffer.put(array);
    }

    private void writeInt(int i) {
        localBuffer.putInt(i);
    }

}
