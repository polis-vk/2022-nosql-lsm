package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final String TABLE_NAME = "SSTable";
    public static final String TEMP = "_TEMP";
    public static final String INDEX = "_i";
    private final Config config;
    private final MemorySegment mapFile;
    private final MemorySegment mapIndex;
    public static final Comparator<MemorySegment> comparator = Utils::compareMemorySegments;
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        Path table = config.basePath().resolve(Path.of(TABLE_NAME));
        if (!Files.exists(table)) {
            mapFile = null;
            mapIndex = null;
            return;
        }
        Path index = table.resolveSibling(TABLE_NAME + INDEX);

        mapFile = map(table, Files.size(table), FileChannel.MapMode.READ_ONLY);
        mapIndex = map(index, Files.size(index), FileChannel.MapMode.READ_ONLY);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return all();
        }
        if (from == null) {
            return allTo(to);
        }
        if (to == null) {
            return allFrom(from);
        }
        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public void flush() throws IOException {
        Path table = config.basePath().resolve(Path.of(TABLE_NAME));
        Path tableTemp = table.resolveSibling(table.getFileName() + TEMP);

        Path index = table.resolveSibling(TABLE_NAME + INDEX);
        Path indexTemp = index.resolveSibling(index.getFileName() + TEMP);

        long sizeInBytes = 0;
        for (Entry<MemorySegment> entry : storage.values()) {
            sizeInBytes += Utils.sizeOfEntry(entry);
        }
        long indexSize = (long) storage.size() * Long.BYTES;

        Files.createFile(tableTemp);
        Files.createFile(indexTemp);
        MemorySegment fileMap = map(tableTemp, sizeInBytes, FileChannel.MapMode.READ_WRITE);
        MemorySegment indexMap = map(indexTemp, indexSize, FileChannel.MapMode.READ_WRITE);

        long indexOffset = 0;
        long fileOffset = 0;

        for (Entry<MemorySegment> entry : storage.values()) {
            MemoryAccess.setLongAtOffset(indexMap, indexOffset, fileOffset);
            indexOffset += Long.BYTES;

            fileOffset += writeSegment(entry.key(), fileMap, fileOffset);

            if (entry.value() == null) {
                MemoryAccess.setLongAtOffset(fileMap, fileOffset, -1);
                fileOffset += Long.BYTES;
                continue;
            }
            fileOffset += writeSegment(entry.value(), fileMap, fileOffset);
        }
        rename(table, tableTemp);
        rename(index, indexTemp);
    }

    private static MemorySegment map(Path table, long length, FileChannel.MapMode mapMode) throws IOException {
        return MemorySegment.mapFile(table,
                0,
                length,
                mapMode,
                ResourceScope.globalScope());
    }

    private long writeSegment(MemorySegment segment, MemorySegment fileMap, long fileOffset) {
        long length = segment.byteSize();
        MemoryAccess.setLongAtOffset(fileMap, fileOffset, length);

        fileMap.asSlice(fileOffset + Long.BYTES).copyFrom(segment);

        return Long.BYTES + length;
    }

    private static void rename(Path table, Path temp) throws IOException {
        Files.deleteIfExists(table);
        Files.move(temp, table, StandardCopyOption.ATOMIC_MOVE);
        Files.deleteIfExists(temp);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = storage.get(key);
        if (entry != null) {
            return entry;
        }
        if (mapFile == null) {
            return null;
        }
        return Utils.binarySearch(key, mapFile, mapIndex);
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        return storage.tailMap(from).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        return storage.headMap(to).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return storage.values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }
}
