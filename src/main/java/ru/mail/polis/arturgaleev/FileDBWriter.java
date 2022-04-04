package ru.mail.polis.arturgaleev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;

public class FileDBWriter implements Closeable {
    public static final String FILE_TMP = "file.tmp";
    private final Path path;
    private final ResourceScope writeScope;

    public FileDBWriter(Path path) {
        this.writeScope = ResourceScope.newConfinedScope();
        this.path = path;
    }

    private static long getMapByteSize(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map) {
        return getIteratorLengthData(map.values().iterator()).second;
    }

    // first value is number of entries, second is byte size
    private static Pair<Long, Long> getIteratorLengthData(Iterator<Entry<MemorySegment>> iterator) {
        long numberOfElements = 0;
        long byteSize = 0;
        while (iterator.hasNext()) {
            numberOfElements++;
            Entry<MemorySegment> entry = iterator.next();
            byteSize += getEntryLength(entry);
        }
        byteSize += Long.BYTES + numberOfElements * Long.BYTES;
        return new Pair<>(numberOfElements, byteSize);
    }

    private static long getEntryLength(Entry<MemorySegment> entry) {
        return entry.key().byteSize()
                + ((entry.value() == null) ? 0 : entry.value().byteSize()) + 2 * Long.BYTES;
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

    private static MemorySegment createTmpMemorySegmentPage(
            long mapByteSize,
            Path tmpPath,
            ResourceScope writeScope
    ) throws IOException {
        Files.deleteIfExists(tmpPath);
        Files.createFile(tmpPath);
        return MemorySegment.mapFile(
                tmpPath,
                0,
                mapByteSize,
                FileChannel.MapMode.READ_WRITE,
                writeScope
        );
    }


    private static void writeIterator(MemorySegment page, long numberOfEntries, Iterator<Entry<MemorySegment>> iterator) {
        MemoryAccess.setLongAtOffset(page, 0, numberOfEntries);

        long dataBeingOffset = Long.BYTES + (long) Long.BYTES * numberOfEntries;
        long i = 0;
        long dataWriteOffset = 0;
        while (iterator.hasNext()) {
            Entry<MemorySegment> entry = iterator.next();
            MemoryAccess.setLongAtOffset(page, Long.BYTES + Long.BYTES * i, dataWriteOffset);

            dataWriteOffset += writeEntry(page, dataBeingOffset + dataWriteOffset, entry);

            i++;
        }
    }

    public void writeMap(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map) throws IOException {
        if (map.isEmpty()) {
            return;
        }

        Iterator<Entry<MemorySegment>> iterator = map.values().iterator();
        long numberOfEntries = map.size();
        long mapByteSize = getMapByteSize(map);

        writeIteratorWithTempFile(iterator, numberOfEntries, mapByteSize);
    }

    // First iterator uses to count length and second to write data

    public void writeIterator(
            Iterator<Entry<MemorySegment>> iterator1,
            Iterator<Entry<MemorySegment>> iterator2
    ) throws IOException {
        if (!iterator1.hasNext()) {
            return;
        }
        Pair<Long, Long> iteratorLengthData = getIteratorLengthData(iterator1);

        writeIteratorWithTempFile(iterator2, iteratorLengthData.first, iteratorLengthData.second);
    }

    private void writeIteratorWithTempFile(Iterator<Entry<MemorySegment>> iterator, long numberOfEntries, long mapByteSize) throws IOException {
        Path tmpPath = path.getParent().resolve(FILE_TMP);
        if (Files.exists(tmpPath)) {
            throw new FileAlreadyExistsException("File " + tmpPath
                    + " already exists. Creation of new one may accuse errors");
        }

        MemorySegment page = createTmpMemorySegmentPage(mapByteSize, tmpPath, writeScope);

        writeIterator(page, numberOfEntries, iterator);

        Files.move(tmpPath, path, StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public void close() throws IOException {
        writeScope.close();
    }

    @SuppressWarnings("UnusedVariable")
    private record Pair<K, V>(K first, V second) {
    }
}
