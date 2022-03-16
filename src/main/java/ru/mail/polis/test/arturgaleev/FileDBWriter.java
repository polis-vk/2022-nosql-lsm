package ru.mail.polis.test.arturgaleev;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentNavigableMap;

public class FileDBWriter implements AutoCloseable {
    private final FileChannel dataChannel;

    public FileDBWriter(Path path, int size) throws IOException {
        dataChannel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    private final void writeInt(int in) throws IOException {
        ByteBuffer buff = ByteBuffer.allocate(4);
        buff.putInt(in);
        buff.flip();
        dataChannel.write(buff);
    }

    protected final void writeEntry(BaseEntry<ByteBuffer> baseEntry) throws IOException {
        int totalSize = baseEntry.key().array().length + baseEntry.value().array().length + 2 * Integer.BYTES;
        ByteBuffer buff = ByteBuffer.allocate(totalSize);
        buff.putInt(baseEntry.key().array().length);
        buff.putInt(baseEntry.value().array().length);
        buff.put(baseEntry.key().array());
        buff.put(baseEntry.value().array());
        buff.flip();
        dataChannel.write(buff);
//        page.putInt(baseEntry.key().array().length);
//        page.putInt(baseEntry.value().array().length);
//        page.put(baseEntry.key());
//        page.put(baseEntry.value());
    }

    public void writeMap(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map) throws IOException {
        int position = 0;
        writeInt(map.size());
//        page.putInt(map.size());
        for (BaseEntry<ByteBuffer> entry : map.values()) {
            writeInt(position);
//            page.putInt(position);
            position += entry.key().array().length + entry.value().array().length + Integer.BYTES * 2;
        }
        for (BaseEntry<ByteBuffer> entry : map.values()) {
            writeEntry(entry);
        }
    }

    @Override
    public void close() throws Exception {
        dataChannel.force(true);
        dataChannel.close();
    }
}

