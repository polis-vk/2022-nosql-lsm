package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
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
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(Utils::compareMemorySegments);

    public InMemoryDao(Config config) {
        this.config = config;
        Path table = config.basePath().resolve(Path.of(TABLE_NAME));
        if (!Files.exists(table)) {
            mapFile = null;
            mapIndex = null;
            return;
        }
        Path index = table.resolveSibling(TABLE_NAME + INDEX);
        try (FileChannel channel = FileChannel.open(table);
             FileChannel indexChannel = FileChannel.open(index)) {

            mapFile = map(table, channel.size());
            mapIndex = map(index, indexChannel.size());

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static MemorySegment map(Path table, long length) throws IOException {
        return MemorySegment.mapFile(table,
                0,
                length,
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.globalScope());
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

        Files.deleteIfExists(tableTemp);
        Files.deleteIfExists(indexTemp);

        Iterator<Entry<MemorySegment>> iterator = storage.values().iterator();
        ByteBuffer forLength = ByteBuffer.allocate(Long.BYTES);
        long offset = 0;
        try (FileChannel channel = open(tableTemp);
             FileChannel indexChannel = open(indexTemp)) {
            while (iterator.hasNext()) {
                Entry<MemorySegment> entry = iterator.next();

                writeLong(forLength, indexChannel, offset);
                offset += Utils.sizeOfEntry(entry);

                writeBuffer(forLength, channel, entry.key());
                if (entry.value() == null) {
                    writeLong(forLength, channel, -1);
                    continue;
                }
                writeBuffer(forLength, channel, entry.value());
            }
            channel.force(false);
            indexChannel.force(false);
        }
        rename(table, tableTemp);
        rename(index, indexTemp);
    }

    private static void rename(Path table, Path temp) throws IOException {
        Files.deleteIfExists(table);
        Files.move(temp, table, StandardCopyOption.ATOMIC_MOVE);
    }

    private void writeBuffer(ByteBuffer forLength, FileChannel channel, MemorySegment value) throws IOException {
        writeLong(forLength, channel, value.byteSize());
        channel.write(value.asByteBuffer());
    }

    private void writeLong(ByteBuffer forLength, FileChannel channel, long value) throws IOException {
        forLength.position(0);
        forLength.putLong(value);
        forLength.position(0);
        channel.write(forLength);
    }

    private static FileChannel open(Path filename) throws IOException {
        return FileChannel.open(filename,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE);
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
