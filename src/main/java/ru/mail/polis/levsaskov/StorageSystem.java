package ru.mail.polis.levsaskov;

import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentNavigableMap;

public final class StorageSystem implements AutoCloseable {
    private static final String COMPACT_PREFIX = "compact.bin";
    private static final String MEM_FILENAME = "daoMem.bin";
    private static final String IND_FILENAME = "daoIndex.bin";

    // Order is important, fresh in begin
    private final List<StoragePart> storageParts;
    private final Path location;

    private StorageSystem(List<StoragePart> storageParts, Path location) {
        this.storageParts = storageParts;
        this.location = location;
    }

    public static StorageSystem load(Path location) throws IOException {
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
        if (storageParts.size() <= 1 && localEntrys.isEmpty()) {
            // Compacted already
            return;
        }

        Path indCompPath = location.resolve(COMPACT_PREFIX + IND_FILENAME);
        Path memCompPath = location.resolve(COMPACT_PREFIX + MEM_FILENAME);
        if (!indCompPath.toFile().createNewFile()
                || !memCompPath.toFile().createNewFile()) {
            throw new FileAlreadyExistsException("Compaction file already exists.");
        }

        // Create compaction part and write all entrys there
        StoragePart compactionPart = StoragePart.load(indCompPath, memCompPath, Integer.MAX_VALUE);
        compactionPart.write(getMergedEntrys(localEntrys, null, null));
        compactionPart.close();

        for (StoragePart storagePart : storageParts) {
            storagePart.delete();
        }
        storageParts.clear();

        // Rename compactionPart
        Path indexFP = getIndexFilePath(0);
        Path memFP = getMemFilePath(0);
        if (!indCompPath.toFile().renameTo(indexFP.toFile())
                || !memCompPath.toFile().renameTo(memFP.toFile())) {
            throw new FileSystemException("Renaming compaction file error.");
        }
        storageParts.add(StoragePart.load(indexFP, memFP, 0));
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

        if (!getIndexFilePath(storageParts.size()).toFile().createNewFile()
                || !getMemFilePath(storageParts.size()).toFile().createNewFile()) {
            throw new FileAlreadyExistsException("Can't create file to save entrys");
        }

        // This part of mem is most fresh, so add in begin
        storageParts.add(0, StoragePart.load(
                getIndexFilePath(storageParts.size()),
                getMemFilePath(storageParts.size()),
                storageParts.size()));

        storageParts.get(0).write(entrys.values().iterator());
    }

    @Override
    public void close() {
        for (StoragePart storagePart : storageParts) {
            storagePart.close();
        }
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
}
