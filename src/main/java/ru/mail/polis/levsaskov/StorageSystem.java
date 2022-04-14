package ru.mail.polis.levsaskov;

import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentNavigableMap;

public final class StorageSystem implements AutoCloseable {
    private static final String MEM_FILENAME = "daoMem.bin";
    private static final String IND_FILENAME = "daoIndex.bin";
    private static final String COMPACTED_PREFIX = "compacted_";
    private static final String COMPACTED_IND_FILE = COMPACTED_PREFIX + IND_FILENAME;
    private static final String COMPACTED_MEM_FILE = COMPACTED_PREFIX + MEM_FILENAME;
    private static final String TMP_PREFIX = "tmp_";
    private static final int NOT_STORAGE_PART = Integer.MAX_VALUE;
    // Order is important, fresh in begin
    private final List<StoragePart> storageParts;
    private final Path location;

    private StorageSystem(List<StoragePart> storageParts, Path location) {
        this.storageParts = storageParts;
        this.location = location;
    }

    public static StorageSystem load(Path location) throws IOException {
        Path compactedIndFile = location.resolve(COMPACTED_IND_FILE);
        Path compactedMemFile = location.resolve(COMPACTED_MEM_FILE);
        if (Files.exists(compactedIndFile) || Files.exists(compactedMemFile)) {
            finishCompact(location, compactedIndFile, compactedMemFile);
        }

        ArrayList<StoragePart> storageParts = new ArrayList<>();

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            Path nextIndFile = getIndexFilePath(location, i);
            Path nextMemFile = getMemFilePath(location, i);
            try {
                storageParts.add(StoragePart.load(nextIndFile, nextMemFile, i));
            } catch (NoSuchFileException e) {
                break;
            }
        }

        // Reverse collection, so fresh is the first
        Collections.reverse(storageParts);
        return new StorageSystem(storageParts, location);
    }

    /**
     * Finds entry with given key in file.
     *
     * @param key - key for entry to find
     * @return entry with the same key or null if there is no entry with the same key
     */
    public Entry<ByteBuffer> findEntry(ByteBuffer key) throws IOException {
        Entry<ByteBuffer> res = null;
        for (StoragePart storagePart : storageParts) {
            res = storagePart.get(key);
            if (res != null) {
                break;
            }
        }

        return res;
    }

    public void compact(ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> localEntrys) throws IOException {
//        System.out.println("Compaction: ");
        Path indCompPath = location.resolve(COMPACTED_IND_FILE);
        Path memCompPath = location.resolve(COMPACTED_MEM_FILE);
        save(indCompPath, memCompPath, getMergedEntrys(localEntrys, null, null));

        // TODO: In stage5 close will be not so simple, so here we won't use close
        close();
        finishCompact(location, indCompPath, memCompPath);

        storageParts.add(StoragePart.load(getIndexFilePath(0), getMemFilePath(0), 0));
    }

    private static void finishCompact(Path location, Path compactedInd, Path compactedMem) throws IOException {
        for (int i = 0; ; i++) {
            Path nextIndFile = getIndexFilePath(location, i);
            Path nextMemFile = getMemFilePath(location, i);
            if (!Files.deleteIfExists(nextIndFile) && !Files.deleteIfExists(nextMemFile)) {
                break;
            }
        }

        Files.move(compactedInd, getIndexFilePath(location, 0), StandardCopyOption.ATOMIC_MOVE);
        Files.move(compactedMem, getMemFilePath(location, 0), StandardCopyOption.ATOMIC_MOVE);
    }

    public Iterator<Entry<ByteBuffer>> getMergedEntrys(
            ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> localEntrys, ByteBuffer from, ByteBuffer to) {
        PriorityQueue<IndexedPeekIterator> binaryHeap = new PriorityQueue<>(
                Comparator.comparing(it -> it.peek().key()));

        for (StoragePart storagePart : storageParts) {
            IndexedPeekIterator peekIterator = storagePart.get(from, to);
            if (peekIterator.peek() != null) {
                binaryHeap.add(peekIterator);
            }
        }

        IndexedPeekIterator localIter = new IndexedPeekIterator(localEntrys.values().iterator(), Integer.MAX_VALUE);
        if (localIter.peek() != null) {
            binaryHeap.add(localIter);
        }

        return new StorageSystemIterator(binaryHeap);
    }

    public void save(ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> entrys) throws IOException {
        if (entrys.isEmpty()) {
            return;
        }
        Path indPath = getIndexFilePath(storageParts.size());
        Path memPath = getMemFilePath(storageParts.size());
        save(indPath, memPath, entrys.values().iterator());
        // This part of mem is most fresh, so add in begin
        storageParts.add(0, StoragePart.load(
                indPath,
                memPath,
                storageParts.size()));
    }

    public boolean isCompacted() {
        return storageParts.size() <= 1;
    }

    @Override
    public void close() {
        for (StoragePart storagePart : storageParts) {
//            System.out.println("Close st part");
            storagePart.close();
        }
        storageParts.clear();
    }

    private Path getMemFilePath(int num) {
        return getMemFilePath(location, num);
    }

    private Path getIndexFilePath(int num) {
        return getIndexFilePath(location, num);
    }

    private static Path getMemFilePath(Path location, int num) {
        return location.resolve(num + MEM_FILENAME);
    }

    private static Path getIndexFilePath(Path location, int num) {
        return location.resolve(num + IND_FILENAME);
    }

    private static void save(Path indPath, Path memPath, Iterator<Entry<ByteBuffer>> entrysToWrite) throws IOException {
        Path indTmpPath = indPath.resolveSibling(TMP_PREFIX + indPath.getFileName());
        Files.deleteIfExists(indTmpPath);
        Files.createFile(indTmpPath);

        Path memTmpPath = memPath.resolveSibling(TMP_PREFIX + memPath.getFileName());
        Files.deleteIfExists(memTmpPath);
        Files.createFile(memTmpPath);

        StoragePart tmpPart = StoragePart.load(indTmpPath, memTmpPath, NOT_STORAGE_PART);
        tmpPart.write(entrysToWrite);
        tmpPart.close();
        Files.move(indTmpPath, indPath, StandardCopyOption.ATOMIC_MOVE);
        Files.move(memTmpPath, memPath, StandardCopyOption.ATOMIC_MOVE);
    }
}
