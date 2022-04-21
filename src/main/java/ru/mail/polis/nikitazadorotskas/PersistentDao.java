package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final Utils utils;
    private final AtomicLong storageSizeInBytes = new AtomicLong(0);
    private final ExecutorService compacter = Executors.newFixedThreadPool(1);
    private final ExecutorService flusher = Executors.newFixedThreadPool(1);
    private final Config config;
    private final ReadWriteLock memoryLock = new ReentrantReadWriteLock();
    private final ReadWriteLock readersLock = new ReentrantReadWriteLock();
    private final ResourceScope scope = ResourceScope.newSharedScope();
    private ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> memory
            = new ConcurrentSkipListMap<>(this::compareMemorySegment);
    private ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> flushingMemory;
    private List<MemorySegmentReader> readers;
    private Future<?> flushTask;
    private Future<?> compactTask;

    public PersistentDao(Config config) throws IOException {
        this.config = config;
        utils = new Utils(config);
        readers = initReaders(config);
    }

    private List<MemorySegmentReader> initReaders(Config config) throws IOException {
        if (config == null) {
            return new ArrayList<>();
        }

        return getReadersOfAllTables();
    }

    private List<MemorySegmentReader> getReadersOfAllTables() throws IOException {
        List<MemorySegmentReader> result = new ArrayList<>();

        int readersSize = utils.countStorageFiles();
        for (int i = 0; i < readersSize; i++) {
            result.add(new MemorySegmentReader(utils, scope, i));
        }

        return result;
    }

    private int compareMemorySegment(MemorySegment first, MemorySegment second) {
        return utils.compareMemorySegment(first, second);
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) throws IOException {
        return new MergedIterator(getIterators(from, to), utils);
    }

    private List<PeekIterator> getIterators(MemorySegment from, MemorySegment to) {
        List<PeekIterator> iterators;
        int numberOfMemoryIterators;

        readersLock.readLock().lock();
        try {
            iterators = getFilesIterators(from, to);
            numberOfMemoryIterators = readers.size();
        } finally {
            readersLock.readLock().unlock();
        }

        if (flushingMemory != null) {
            iterators.add(getPeekIterator(numberOfMemoryIterators++, from, to, flushingMemory));
        }

        memoryLock.readLock().lock();
        try {
            iterators.add(getPeekIterator(numberOfMemoryIterators, from, to, memory));
        } finally {
            memoryLock.readLock().unlock();
        }
        return iterators;
    }

    private List<PeekIterator> getFilesIterators(MemorySegment from, MemorySegment to) {
        List<PeekIterator> iterators = new ArrayList<>();
        for (MemorySegmentReader reader : readers) {
            iterators.add(reader.getFromDisk(from, to));
        }

        return iterators;
    }

    private PeekIterator getPeekIterator(
            int number,
            MemorySegment from,
            MemorySegment to,
            ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> source
    ) {
        return new PeekIterator(number, getMap(from, to, source).values().iterator());
    }

    private ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> getMap(
            MemorySegment from, MemorySegment to, ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> map
    ) {
        if (from == null && to == null) {
            return map;
        }

        if (from == null) {
            return map.headMap(to);
        }

        if (to == null) {
            return map.tailMap(from);
        }

        return map.subMap(from, to);
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) throws IOException {
        BaseEntry<MemorySegment> result;

        memoryLock.readLock().lock();
        try {
            result = memory.get(key);
        } finally {
            memoryLock.readLock().unlock();
        }

        if (result == null && flushingMemory != null) {
            result = flushingMemory.get(key);
        }

        if (result != null) {
            return utils.checkIfWasDeleted(result);
        }

        readersLock.readLock().lock();
        try {
            if (readers.size() == 0) {
                return null;
            }

            return getFromStorage(key);
        } finally {
            readersLock.readLock().unlock();
        }
    }

    private BaseEntry<MemorySegment> getFromStorage(MemorySegment key) {
        for (int i = readers.size() - 1; i >= 0; i--) {
            BaseEntry<MemorySegment> res = readers.get(i).getFromDisk(key);
            if (res != null) {
                return utils.checkIfWasDeleted(res);
            }
        }

        return null;
    }

    @Override
    public void compact() throws IOException {
        if (!scope.isAlive()) {
            throw new IllegalStateException("called compact after close");
        }

        compactTask = compacter.submit(this::doCompact);
    }

    private void doCompact() {
        int currentFilesNumber = readers.size();
        if (currentFilesNumber == 0) {
            return;
        }

        int entriesCount = 0;
        long byteSize = 0;

        Iterator<BaseEntry<MemorySegment>> allEntries = allFilesIterator();
        while (allEntries.hasNext()) {
            entriesCount++;
            byteSize += byteSizeOfEntry(allEntries.next());
        }

        try {
            writeToTmpFile(entriesCount, byteSize);
            deleteOldFilesAndMoveCompactFile(currentFilesNumber);

            readersLock.writeLock().lock();
            try {
                moveNewFlushedFiles(currentFilesNumber, readers.size());
                readers = initReaders(config);
            } finally {
                readersLock.writeLock().unlock();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Iterator<BaseEntry<MemorySegment>> allFilesIterator() {
        readersLock.readLock().lock();
        try {
            return new MergedIterator(getFilesIterators(null, null), utils);
        } finally {
            readersLock.readLock().unlock();
        }
    }

    private void moveNewFlushedFiles(int oldFilesNumber, int newFilesNumber) throws IOException {
        for (int i = oldFilesNumber; i < newFilesNumber; i++) {
            Files.move(
                    utils.getStoragePath(oldFilesNumber),
                    utils.getStoragePath(i - oldFilesNumber + 1),
                    StandardCopyOption.ATOMIC_MOVE);
        }
    }

    private void writeToTmpFile(int entriesCount, long byteSize) throws IOException {
        Files.deleteIfExists(utils.getStoragePath(Utils.TMP_COMPACTED_FILE_NUMBER));
        flush(allFilesIterator(), Utils.TMP_COMPACTED_FILE_NUMBER, entriesCount, byteSize);
    }

    private void deleteOldFilesAndMoveCompactFile(int oldFilesNumber) throws IOException {
        utils.deleteStorageFiles(oldFilesNumber);
        Files.move(
                utils.getStoragePath(Utils.TMP_COMPACTED_FILE_NUMBER),
                utils.getStoragePath(0),
                StandardCopyOption.ATOMIC_MOVE);
    }

    private long byteSizeOfEntry(BaseEntry<MemorySegment> entry) {
        long valueSize = entry.value() == null ? 0L : entry.value().byteSize();
        return entry.key().byteSize() + valueSize;
    }

    private void flush(Iterator<BaseEntry<MemorySegment>> values, int fileIndex, int entriesCount, long byteSize)
            throws IOException {
        try (ResourceScope confinedScope = ResourceScope.newConfinedScope()) {
            MemorySegmentWriter segmentWriter = new MemorySegmentWriter(
                    entriesCount,
                    byteSize,
                    utils,
                    confinedScope,
                    fileIndex
            );
            while (values.hasNext()) {
                segmentWriter.writeEntry(values.next());
            }
        }
    }

    @Override
    public void flush() throws IOException {
        if (memory.isEmpty()) {
            return;
        }

        if (flushTask != null && !flushTask.isDone()) {
            throw new IOException("Too many flushes: one table is being written and one is full");
        }

        flushingMemory = memory;

        long storageSize = storageSizeInBytes.get();
        flushTask = flusher.submit(() -> doFlush(storageSize));
        memory = new ConcurrentSkipListMap<>(this::compareMemorySegment);
        storageSizeInBytes.set(0);
    }

    private void doFlush(long storageSize) {
        try {
            Files.deleteIfExists(utils.getStoragePath(Utils.TMP_FILE_NUMBER));
            flush(flushingMemory.values().iterator(),
                    Utils.TMP_FILE_NUMBER,
                    flushingMemory.size(),
                    storageSize);
            Files.move(
                    utils.getStoragePath(Utils.TMP_FILE_NUMBER),
                    utils.getStoragePath(readers.size()),
                    StandardCopyOption.ATOMIC_MOVE);
            if (scope.isAlive()) {
                readersLock.writeLock().lock();
                try {
                    readers.add(new MemorySegmentReader(utils, scope, readers.size()));
                } finally {
                    readersLock.writeLock().unlock();
                }
            }
            flushingMemory = null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        storageSizeInBytes.addAndGet(byteSizeOfEntry(entry));

        memoryLock.writeLock().lock();
        try {
            memory.put(entry.key(), entry);
        } finally {
            memoryLock.writeLock().unlock();
        }

        try {
            if (storageSizeInBytes.get() >= config.flushThresholdBytes()) {
                flush();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (!scope.isAlive()) {
            return;
        }

        flush();

        try {
            waitTask(compactTask);
            scope.close();
            waitTask(flushTask);
        } catch (ExecutionException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
        flusher.shutdown();
        compacter.shutdown();

        memory = null;
    }

    private void waitTask(Future<?> task) throws ExecutionException, InterruptedException {
        if (task != null) {
            task.get();
        }
    }
}
