package ru.mail.polis.arturgaleev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentNavigableMap;

public class FileDBWriter implements Closeable {
    private final Path path;
    private final ResourceScope writeScope;
    private MemorySegment page;

    public FileDBWriter(Path path) {
        this.writeScope = ResourceScope.newConfinedScope();
        this.path = path;
    }

    static long getMapByteSize(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map) {
        final long[] sz = {Long.BYTES + (long) map.size() * Long.BYTES};
        map.forEach((key, val) ->
                sz[0] = sz[0] + key.byteSize()
                        + ((val.value() == null) ? 0 : val.value().byteSize()) + 2 * Long.BYTES
        );
        return sz[0];
    }

    private static long writeEntry(MemorySegment page, long posToWrite, Entry<MemorySegment> baseEntry) {
        long offset = 0;

        MemoryAccess.setLongAtOffset(page, posToWrite + offset, baseEntry.key().byteSize());
        offset += Long.BYTES;
        MemoryAccess.setLongAtOffset(page, posToWrite + offset,
                baseEntry.value() == null ? -1 : baseEntry.value().byteSize());
        offset += Long.BYTES;

        page.asSlice(posToWrite + offset, baseEntry.key().byteSize()).copyFrom(baseEntry.key());
        offset += baseEntry.key().byteSize();
        if (baseEntry.value() != null) {
            page.asSlice(posToWrite + offset, baseEntry.value().byteSize()).copyFrom(baseEntry.value());
            offset += baseEntry.value().byteSize();
        }
        return offset;
    }

    public void writeMap(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map) throws IOException {
        createMap(map);
        MemoryAccess.setLongAtOffset(page, 0, map.size());
        long i = 0;
        long linksOffset = 0;
        for (Entry<MemorySegment> entry : map.values()) {
            MemoryAccess.setLongAtOffset(page, Long.BYTES + Long.BYTES * i, linksOffset);
            linksOffset += 2 * Long.BYTES;
            linksOffset += entry.key().byteSize();
            linksOffset += entry.value() == null ? 0 : entry.value().byteSize();
            i++;
        }
        long offset = Long.BYTES + (long) Long.BYTES * map.size();
        for (Entry<MemorySegment> entry : map.values()) {
            offset += writeEntry(page, offset, entry);
        }
    }

    @Override
    public void close() throws IOException {
        writeScope.close();
    }

    private void createMap(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map) throws IOException {
        if (page == null) {
            Files.deleteIfExists(path);
            Files.createFile(path);
            page = MemorySegment.mapFile(
                    path,
                    0,
                    getMapByteSize(map),
                    FileChannel.MapMode.READ_WRITE,
                    writeScope
            );
        }
    }
}

