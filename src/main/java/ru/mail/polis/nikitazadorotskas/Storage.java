package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class Storage {
    private final Utils utils;
    private final Config config;
    private List<MemorySegmentReader> readers;
    private ResourceScope scope = ResourceScope.newSharedScope();

    Storage(Config config) throws IOException {
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

    List<PeekIterator> getFilesIterators(MemorySegment from, MemorySegment to) {
        List<PeekIterator> iterators = new ArrayList<>();
        for (MemorySegmentReader reader : readers) {
            iterators.add(reader.getFromDisk(from, to));
        }

        return iterators;
    }

    BaseEntry<MemorySegment> getFromStorage(MemorySegment key) {
        for (int i = readers.size() - 1; i >= 0; i--) {
            BaseEntry<MemorySegment> res = readers.get(i).getFromDisk(key);
            if (res != null) {
                return utils.checkIfWasDeleted(res);
            }
        }

        return null;
    }

    synchronized void doFlush(Memory flushingMemory) {
        try {
            Files.deleteIfExists(utils.getStoragePath(Utils.TMP_FILE_NUMBER));
            flush(flushingMemory.getIterator(),
                    Utils.TMP_FILE_NUMBER,
                    flushingMemory.size(),
                    flushingMemory.getBytesSize());
            Files.move(
                    utils.getStoragePath(Utils.TMP_FILE_NUMBER),
                    utils.getStoragePath(readers.size()),
                    StandardCopyOption.ATOMIC_MOVE);
            if (scope.isAlive()) {
                readers.add(new MemorySegmentReader(utils, scope, readers.size()));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

    void doCompact() {
        int currentFilesNumber = size();
        if (currentFilesNumber == 0) {
            return;
        }

        int entriesCount = 0;
        long byteSize = 0;

        Iterator<BaseEntry<MemorySegment>> allEntries = allFilesIterator();
        while (allEntries.hasNext()) {
            entriesCount++;
            byteSize += Utils.byteSizeOfEntry(allEntries.next());
        }

        try {
            writeToTmpFile(entriesCount, byteSize);
            scope.close();
            scope = ResourceScope.newSharedScope();
            deleteOldFilesAndMoveCompactFile(currentFilesNumber);

            readers = initReaders(config);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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

    private Iterator<BaseEntry<MemorySegment>> allFilesIterator() {
        return new MergedIterator(this::getFilesIterators, null, null, utils);
    }

    int size() {
        return readers.size();
    }

    boolean isNotAlive() {
        return !scope.isAlive();
    }

    void close() {
        scope.close();
    }
}
