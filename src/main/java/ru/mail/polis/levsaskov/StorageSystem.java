package ru.mail.polis.levsaskov;

import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

public class StorageSystem implements AutoCloseable {
    private static final String MEM_FILENAME = "daoMem.bin";
    private static final String INDEX_FILENAME = "daoIndex.bin";

    // Order is important, fresh in begin
    private final ArrayList<StoragePart> storageParts;
    private final Path location;

    private StorageSystem(ArrayList<StoragePart> storageParts, Path location) {
        this.storageParts = storageParts;
        this.location = location;
    }

    public static StorageSystem load(Path location) throws IOException {
        ArrayList<StoragePart> storageParts = new ArrayList<>();

        for (int i = 0; ; i++) {
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
        if (entrys.size() == 0) {
            return;
        }

        if (!getIndexFilePath(storageParts.size()).toFile().createNewFile() ||
                !getMemFilePath(storageParts.size()).toFile().createNewFile()) {
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
        return location.resolve(num + INDEX_FILENAME);
    }
}
