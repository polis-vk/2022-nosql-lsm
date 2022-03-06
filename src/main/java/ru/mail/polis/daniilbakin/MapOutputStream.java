package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class MapOutputStream extends FileOutputStream {

    protected MapOutputStream(File name) throws FileNotFoundException {
        super(name);
    }

    /**
     * Return: array of indexes objects location.
     */
    protected int[] writeMap(Map<ByteBuffer, BaseEntry<ByteBuffer>> data) throws IOException {
        int[] indexes = new int[data.size()];
        int i = 0;
        int indexObjPosition = 0;
        for (Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>> entry : data.entrySet()) {
            indexes[i++] = indexObjPosition;
            ByteBuffer localBuffer = ByteBuffer.allocate(
                    entry.getKey().capacity() + entry.getValue().value().capacity() + Integer.BYTES * 2
            );
            writeEntry(entry, localBuffer);
            indexObjPosition += localBuffer.capacity();
            byte[] array = new byte[localBuffer.capacity()];
            localBuffer.flip();
            localBuffer.get(array);
            write(array);
        }
        return indexes;
    }

    private void writeEntry(Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>> entry, ByteBuffer localBuffer) {
        writeByteBuffer(entry.getKey(), localBuffer);
        writeByteBuffer(entry.getValue().value(), localBuffer);
    }

    private void writeByteBuffer(ByteBuffer buffer, ByteBuffer localBuffer) {
        buffer.position(buffer.arrayOffset());
        byte[] array = buffer.array();
        writeInt(array.length, localBuffer);
        localBuffer.put(array);
    }

    private void writeInt(int i, ByteBuffer localBuffer) {
        localBuffer.putInt(i);
    }

}
