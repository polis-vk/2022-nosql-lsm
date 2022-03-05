package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final String TABLE_NAME = "SSTable";
    private final Config config;
    private final Comparator<MemorySegment> comparator = this::compareMemorySegments;
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);

    public InMemoryDao(Config config) {
        this.config = config;
        Path table = config.basePath().resolve(Path.of(TABLE_NAME));
        if (!Files.exists(table)) {
            return;
        }
        ByteBuffer forLength = ByteBuffer.allocate(Long.BYTES);
        try (FileChannel channel = FileChannel.open(table)) {
            while (channel.position() != channel.size()) {
                long keyLength = getLength(forLength, channel);
                MemorySegment key = getMemorySegment(table, channel, keyLength);
                long valueLength = getLength(forLength, channel);
                if (valueLength == -1) {
                    storage.put(key, new BaseEntry<>(key, null));
                    continue;
                }
                MemorySegment value = getMemorySegment(table, channel, valueLength);
                storage.put(key, new BaseEntry<>(key, value));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private MemorySegment getMemorySegment(Path table, FileChannel channel, long length) throws IOException {
        MemorySegment key = MemorySegment.mapFile(table,
                channel.position(),
                length,
                FileChannel.MapMode.READ_ONLY,
                ResourceScope.globalScope());
        channel.position(channel.position() + length);
        return key;
    }

    private long getLength(ByteBuffer forLength, FileChannel channel) throws IOException {
        forLength.position(0);
        channel.read(forLength);
        forLength.position(0);
        return forLength.getLong();
    }

    private int compareMemorySegments(MemorySegment o1, MemorySegment o2) {
        long mismatch = o1.mismatch(o2);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == o1.byteSize()) {
            return -1;
        }
        if (mismatch == o2.byteSize()) {
            return 1;
        }
        byte b1 = MemoryAccess.getByteAtOffset(o1, mismatch);
        byte b2 = MemoryAccess.getByteAtOffset(o2, mismatch);
        return Byte.compare(b1, b2);
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
        Path ssTable = config.basePath().resolve(Path.of(TABLE_NAME));
        Iterator<Entry<MemorySegment>> iterator = storage.values().iterator();
        ByteBuffer forLength = ByteBuffer.allocate(Long.BYTES);
        try (FileChannel channel = open(ssTable)) {
            while (iterator.hasNext()) {
                Entry<MemorySegment> entry = iterator.next();
                writeBuffer(forLength, channel, entry.key());
                if (entry.value() == null) {
                    writeLength(forLength, channel, -1);
                    continue;
                }
                writeBuffer(forLength, channel, entry.value());
            }
            channel.force(false);
        }
    }

    private void writeBuffer(ByteBuffer forLength, FileChannel channel, MemorySegment value) throws IOException {
        writeLength(forLength, channel, value.byteSize());
        channel.write(value.asByteBuffer());
    }

    private void writeLength(ByteBuffer forLength, FileChannel channel, long length) throws IOException {
        forLength.position(0);
        forLength.putLong(length);
        forLength.position(0);
        channel.write(forLength);
    }

    private static FileChannel open(Path filename) throws IOException {
        return FileChannel.open(filename,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
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
