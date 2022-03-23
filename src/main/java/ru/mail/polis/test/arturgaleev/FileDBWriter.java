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
    private MappedByteBuffer page = null;
    private final FileChannel dataChannel;

    public FileDBWriter(Path path) throws IOException {
        dataChannel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    static int getMapByteSize(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map) {
        final int[] sz = {Integer.BYTES + map.size() * Integer.BYTES};
        map.forEach((key, val) -> sz[0] += key.limit() + ((val.value() != null) ? val.value().limit() : 0) + 2 * Integer.BYTES);
        return sz[0];
    }

    protected final void writeEntry(BaseEntry<ByteBuffer> baseEntry) {
        page.putInt(baseEntry.key().array().length);
        page.putInt(baseEntry.value() != null ? baseEntry.value().array().length : -1);
        page.put(baseEntry.key());
        if (baseEntry.value() != null) {
            page.put(baseEntry.value());
        }
    }

    public void writeMap(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map) throws IOException {
        createMap(map);
        int position = 0;
        page.putInt(map.size());
        for (BaseEntry<ByteBuffer> entry : map.values()) {
            page.putInt(position);
            position += entry.key().array().length + ((entry.value() != null) ? entry.value().array().length : 0) + Integer.BYTES * 2;
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

    private void createMap(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map) throws IOException {
        if (page == null) {
            page = dataChannel.map(FileChannel.MapMode.READ_WRITE, 0, getMapByteSize(map));
        }
    }
}

