package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class PersistentDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> memory
            = new ConcurrentSkipListMap<>(this::compareMemorySegment);
    private final AtomicLong storageSizeInBytes = new AtomicLong(0);
    private final MemorySegmentReader[] readers;
    private final Utils utils;
    private final ResourceScope scope = ResourceScope.newSharedScope();

    public PersistentDao(Config config) throws IOException {
        utils = new Utils(config);

        readers = initReaders(config);
    }

    private MemorySegmentReader[] initReaders(Config config) throws IOException {
        if (config == null) {
            return new MemorySegmentReader[0];
        }

        try {
            return getReaderOfCompactedTable();
        } catch (NoSuchFileException e) {
            return getReadersOfAllTables();
        }
    }

    @SuppressWarnings("DuplicateThrows")
    private MemorySegmentReader[] getReaderOfCompactedTable() throws NoSuchFileException, IOException {
        return new MemorySegmentReader[]{new MemorySegmentReader(utils, scope, Utils.COMPACTED_FILE_NUMBER)};
    }

    private MemorySegmentReader[] getReadersOfAllTables() throws IOException {
        MemorySegmentReader[] result = new MemorySegmentReader[utils.countStorageFiles()];

        for (int i = 0; i < result.length; i++) {
            result[i] = new MemorySegmentReader(utils, scope, i);
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
        List<PeekIterator> iterators = new ArrayList<>();
        for (MemorySegmentReader reader : readers) {
            iterators.add(reader.getFromDisk(from, to));
        }
        iterators.add(new PeekIterator(readers.length, getMap(from, to).values().iterator()));

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
        BaseEntry<MemorySegment> result = memory.get(key);

        if (result != null) {
            return utils.checkIfWasDeleted(result);
        }

        if (readers.length == 0) {
            return null;
        }

        for (int i = readers.length - 1; i >= 0; i--) {
            BaseEntry<MemorySegment> res = readers[i].getFromDisk(key);
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

        int entriesCount = 0;
        long byteSize = 0;
        Iterator<BaseEntry<MemorySegment>> allEntries = all();
        while (allEntries.hasNext()) {
            entriesCount++;
            byteSize += byteSizeOfEntry(allEntries.next());
        }

        writeToTmpFile(entriesCount, byteSize);
        //tmp файл читать не будем, поскольку он, возможно, не успел до конца записаться

        scope.close();
        memory.clear();

        moveTmpFileToCompactedFile();
        //если прервались здесь или далее во время удаления,
        //то будем читать только новый файл, помеченный как компактный, старые файлы не читаем,
        //однако они не будут удаляться до следующего вызова compact/flush

        deleteOldFilesAndMoveCompactFile();
        //теперь остался 1 файл
    }

    private void writeToTmpFile(int entriesCount, long byteSize) throws IOException {
        Files.deleteIfExists(utils.getStoragePath(Utils.TMP_FILE_NUMBER));
        flush(all(), Utils.TMP_FILE_NUMBER, entriesCount, byteSize);
    }

    private void moveTmpFileToCompactedFile() throws IOException {
        Files.move(
                utils.getStoragePath(Utils.TMP_FILE_NUMBER),
                utils.getStoragePath(Utils.COMPACTED_FILE_NUMBER),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    private void deleteOldFilesAndMoveCompactFile() throws IOException {
        utils.deleteStorageFiles();
        Files.move(
                utils.getStoragePath(Utils.COMPACTED_FILE_NUMBER),
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
        if (readers.length == 1 && readers[0].getNumber() == Utils.COMPACTED_FILE_NUMBER) {
            deleteOldFilesAndMoveCompactFile();
        }
        flush(memory.values().iterator(), readers.length, memory.size(), storageSizeInBytes.get());
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        storageSizeInBytes.addAndGet(byteSizeOfEntry(entry));
        memory.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        if (!scope.isAlive()) {
            return;
        }
        scope.close();

        flush();
        memory.clear();
    }
}
