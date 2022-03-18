package ru.mail.polis.artyomdrozdov;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final MemorySegment VERY_FIRST_KEY = MemorySegment.ofArray(new byte[]{});

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(
            (m1, m2) -> {
                long firstMismatch = m1.mismatch(m2);
                if (firstMismatch == -1) {
                    return 0;
                }
                if (firstMismatch == m1.byteSize()) {
                    return -1;
                }
                if (firstMismatch == m2.byteSize()) {
                    return 1;
                }
                return Byte.compareUnsigned(
                        MemoryAccess.getByteAtOffset(m1, firstMismatch),
                        MemoryAccess.getByteAtOffset(m2, firstMismatch)
                );
            }
    );

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Config config;
    private final MemorySegment readPage;

    private final ResourceScope scope = ResourceScope.newSharedScope();

    public MemorySegmentDao(Config config) throws IOException {
        this.config = config;
        Path path = getDataPath();
        MemorySegment page;
        try {
            long size = Files.size(path);

            page = MemorySegment.mapFile(path, 0, size, FileChannel.MapMode.READ_ONLY, scope);
        } catch (NoSuchFileException e) {
            page = null;
        }

        readPage = page;
    }

    private Path getDataPath() {
        return config.basePath().resolve("data.dat");
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        lock.readLock().lock();
        try {
            if (from == null) {
                from = VERY_FIRST_KEY;
            }

            if (to == null) {
                return storage.tailMap(from).values().iterator();
            }

            return storage.subMap(from, to).values().iterator();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Entry<MemorySegment> get(@NotNull MemorySegment key) {
        lock.readLock().lock();
        try {
            Entry<MemorySegment> entry = storage.get(key);
            if (entry != null) {
                return entry;
            }

            if (readPage == null) {
                return null;
            }

            long offset = 0;

            while (offset < readPage.byteSize()) {
                long keySize = MemoryAccess.getLongAtOffset(readPage, offset);
                offset += Long.BYTES;
                long valueSize = MemoryAccess.getLongAtOffset(readPage, offset);
                offset += Long.BYTES;

                if (keySize != key.byteSize()) {
                    offset += keySize + valueSize;
                    continue;
                }

                MemorySegment currentKey = readPage.asSlice(offset, keySize);
                if (key.mismatch(currentKey) == -1) {
                    return new BaseEntry<>(key, readPage.asSlice(offset + keySize, valueSize));
                }
                offset += keySize + valueSize;
            }

            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        lock.readLock().lock();
        try {
            storage.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void close() throws IOException {
        if (!scope.isAlive()) {
            return;
        }
        scope.close();

        lock.writeLock().lock();
        try (ResourceScope writeScope = ResourceScope.newConfinedScope()) {
            long size = 0;
            for (Entry<MemorySegment> value : storage.values()) {
                size += value.value().byteSize() + value.key().byteSize();
            }
            size += 2L * storage.size() * Long.BYTES;

            Files.deleteIfExists(getDataPath());
            Files.createFile(getDataPath());
            MemorySegment page =
                        MemorySegment.mapFile(
                                getDataPath(),
                                0,
                                size,
                                FileChannel.MapMode.READ_WRITE,
                                writeScope);

            long offset = 0;
            for (Entry<MemorySegment> value : storage.values()) {
                MemoryAccess.setLongAtOffset(page, offset, value.key().byteSize());
                offset += Long.BYTES;
                MemoryAccess.setLongAtOffset(page, offset, value.value().byteSize());
                offset += Long.BYTES;

                page.asSlice(offset).copyFrom(value.key());
                offset += value.key().byteSize();

                page.asSlice(offset).copyFrom(value.value());
                offset += value.value().byteSize();
            }

        } finally {
            lock.writeLock().unlock();
        }
    }
}
