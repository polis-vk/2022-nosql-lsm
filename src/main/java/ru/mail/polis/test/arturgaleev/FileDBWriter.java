package ru.mail.polis.test.arturgaleev;

import ru.mail.polis.BaseEntry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentNavigableMap;

public class FileDBWriter implements Closeable {
    private MappedByteBuffer page;
    private final FileChannel dataChannel;

    public FileDBWriter(Path path) throws IOException {
        dataChannel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    static int getMapByteSize(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map) {
        final int[] sz = {Integer.BYTES + map.size() * Integer.BYTES};
        map.forEach((key, val) ->
                sz[0] = sz[0] + key.limit()
                        + ((val.value() == null) ? 0 : val.value().limit()) + 2 * Integer.BYTES
        );
        return sz[0];
    }

    protected final void writeEntry(BaseEntry<ByteBuffer> baseEntry) {
        page.putInt(baseEntry.key().array().length);
        page.putInt(baseEntry.value() == null ? -1 : baseEntry.value().array().length);
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
            position += entry.key().array().length
                    + ((entry.value() == null) ? 0 : entry.value().array().length)
                    + Integer.BYTES * 2;
        }
        for (BaseEntry<ByteBuffer> entry : map.values()) {
            writeEntry(entry);
        }
    }

    @Override
    public void close() throws IOException {
        page.force();
        dataChannel.close();
    }

    private void createMap(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map) throws IOException {
        if (page == null) {
            page = dataChannel.map(FileChannel.MapMode.READ_WRITE, 0, getMapByteSize(map));
        }
    }
}

