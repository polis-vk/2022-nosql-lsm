package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> memory
            = new ConcurrentSkipListMap<>(this::compareMemorySegment);
    private final AtomicLong storageSizeInBytes = new AtomicLong(0);
    private final MemorySegmentReader[] readers;
    private final Utils utils;
    private final ResourceScope scope = ResourceScope.newSharedScope();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final int numberOfFiles;

    public PersistentDao(Config config) throws IOException {
        utils = new Utils(config);

        numberOfFiles = getNumberOfFiles(config);
        readers = new MemorySegmentReader[numberOfFiles];

        for (int i = 0; i < numberOfFiles; i++) {
            readers[i] = new MemorySegmentReader(utils, scope, i);
        }
    }

    private int getNumberOfFiles(Config config) throws IOException {
        if (config == null) {
            return 0;
        }

        try {
            return (int) Files.list(config.basePath()).count() / 2;
        } catch (NoSuchFileException e) {
            return 0;
        }
    }

    private int compareMemorySegment(MemorySegment first, MemorySegment second) {
        return utils.compareMemorySegment(first, second);
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) throws IOException {
        lock.readLock().lock();
        try {
            return new MergedIterator(getIterators(from, to), utils);
        } finally {
            lock.readLock().unlock();
        }
    }

    private List<PeekIterator> getIterators(MemorySegment from, MemorySegment to) {
        List<PeekIterator> iterators = new ArrayList<>();
        for (MemorySegmentReader reader : readers) {
            iterators.add(reader.getFromDisk(from, to));
        }
        iterators.add(new PeekIterator(numberOfFiles, getMap(from, to).values().iterator()));

        return iterators;
    }

    private ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> getMap(
            MemorySegment from, MemorySegment to
    ) {
        if (from == null && to == null) {
            return memory;
        }

        if (from == null) {
            return memory.headMap(to);
        }

        if (to == null) {
            return memory.tailMap(from);
        }

        return memory.subMap(from, to);
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) throws IOException {
        lock.readLock().lock();
        try {
            BaseEntry<MemorySegment> result = memory.get(key);

            if (result != null) {
                return checkIfWasDeleted(result);
            }

            if (readers.length == 0) {
                return null;
            }

            for (int i = readers.length - 1; i >= 0; i--) {
                BaseEntry<MemorySegment> res = readers[i].getFromDisk(key);
                if (res != null) {
                    return checkIfWasDeleted(res);
                }
            }

            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    private BaseEntry<MemorySegment> checkIfWasDeleted(BaseEntry<MemorySegment> entry) {
        return entry.value() == null ? null : entry;
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        lock.readLock().lock();
        try {
            long valueSize = entry.value() == null ? 0L : entry.value().byteSize();
            storageSizeInBytes.addAndGet(entry.key().byteSize() + valueSize);
            memory.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (!scope.isAlive()) {
            return;
        }
        scope.close();

        lock.writeLock().lock();
        try (ResourceScope confinedScope = ResourceScope.newConfinedScope()) {
            utils.createFilesIfNotExist(numberOfFiles);
            MemorySegmentWriter segmentWriter = new MemorySegmentWriter(
                    memory.size(),
                    storageSizeInBytes.get(),
                    utils,
                    confinedScope,
                    numberOfFiles
            );
            for (BaseEntry<MemorySegment> entry : memory.values()) {
                segmentWriter.writeEntry(entry);
            }
        } finally {
            lock.writeLock().unlock();
        }

        memory.clear();
    }
}
