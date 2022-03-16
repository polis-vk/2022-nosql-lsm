package ru.mail.polis.test.arturgaleev;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentNavigableMap;

public class FileDBWriter implements AutoCloseable {
    private final MappedByteBuffer page;
    private final FileChannel dataChannel;

    public FileDBWriter(Path path, int size) throws IOException {
        dataChannel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ,
                StandardOpenOption.TRUNCATE_EXISTING);
        page = dataChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
    }

    protected final void writeEntry(BaseEntry<ByteBuffer> baseEntry) {
        page.putInt(baseEntry.key().array().length);
        page.putInt(baseEntry.value().array().length);
        page.put(baseEntry.key());
        page.put(baseEntry.value());
    }

    public void writeMap(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map) {
        int position = 0;
        page.putInt(map.size());
        for (BaseEntry<ByteBuffer> entry : map.values()) {
            page.putInt(position);
            position += entry.key().array().length + entry.value().array().length + Integer.BYTES * 2;
        }
        for (BaseEntry<ByteBuffer> entry : map.values()) {
            writeEntry(entry);
        }
    }

    @Override
    public void close() throws Exception {
        page.force();
        dataChannel.close();
    }
}

