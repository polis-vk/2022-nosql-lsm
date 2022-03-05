package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class MapOutputStream extends FileOutputStream {

    public MapOutputStream(String name) throws FileNotFoundException {
        super(name);
    }

    public void writeMap(Map<ByteBuffer, BaseEntry<ByteBuffer>> data) throws IOException {
        for (Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>> entry : data.entrySet()) {
            writeEntry(entry);
        }
    }

    private void writeEntry(Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>> entry) throws IOException {
        writeByteBuffer(entry.getKey());
        writeBaseEntry(entry.getValue());
    }

    private void writeBaseEntry(BaseEntry<ByteBuffer> entry) throws IOException {
        if (entry == null) {
            writeInt(-1);
            return;
        }
        writeByteBuffer(entry.key());
        writeByteBuffer(entry.value());
    }

    private void writeByteBuffer(ByteBuffer buffer) throws IOException {
        if (buffer == null) {
            writeInt(-1);
            return;
        }
        buffer.position(buffer.arrayOffset());
        byte[] array = buffer.array();
        writeInt(array.length);
        write(array);
    }

    private void writeInt(Integer i) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(i);
        write(buffer.array());
    }

}
