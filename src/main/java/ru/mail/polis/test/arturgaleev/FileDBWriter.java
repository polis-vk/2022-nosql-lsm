package ru.mail.polis.test.arturgaleev;

import ru.mail.polis.BaseEntry;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentNavigableMap;

public class FileDBWriter extends FileOutputStream {

    private final byte[] writeBuffer = new byte[4];
    protected int written;

    public FileDBWriter(String name) throws FileNotFoundException {
        super(name);
    }

    protected final void writeInt(int v) throws IOException {
        writeBuffer[0] = (byte) (v >>> 24);
        writeBuffer[1] = (byte) (v >>> 16);
        writeBuffer[2] = (byte) (v >>> 8);
        writeBuffer[3] = (byte) (v);
        super.write(writeBuffer, 0, 4);
        incCount(Integer.BYTES);
    }

    protected final void writeEntry(BaseEntry<ByteBuffer> baseEntry) throws IOException {
        int totalSize = baseEntry.key().array().length + baseEntry.value().array().length + 2 * Integer.BYTES;
        ByteBuffer buff = ByteBuffer.allocate(totalSize);
        buff.putInt(baseEntry.key().array().length);
        buff.putInt(baseEntry.value().array().length);
        buff.put(baseEntry.key().array());
        buff.put(baseEntry.value().array());
        super.write(buff.array());
        incCount(totalSize);
    }

    public void writeMap(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map) throws IOException {
        int position = 0;
        writeInt(map.size());
        for (BaseEntry<ByteBuffer> entry : map.values()) {
            writeInt(position);
            position += entry.key().array().length + entry.value().array().length + Integer.BYTES * 2;
        }
        for (BaseEntry<ByteBuffer> entry : map.values()) {
            writeEntry(entry);
        }
    }

    private void incCount(int value) {
        int temp = written + value;
        if (temp < 0) {
            temp = Integer.MAX_VALUE;
        }
        written = temp;
    }
}

