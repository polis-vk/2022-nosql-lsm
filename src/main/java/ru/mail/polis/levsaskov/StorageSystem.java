package ru.mail.polis.levsaskov;

import jdk.incubator.foreign.MemoryAccess;
import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageSystem implements AutoCloseable {
    private static final int DEFAULT_ALLOC_SIZE = 2048;
    private static final int BYTES_IN_INT = 4;
    private static final int BYTES_IN_LONG = 8;
    private static final String MEM_FILENAME = "daoMem.bin";
    private static final String INDEX_FILENAME = "daoIndex.bin";

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private int storagePartsC;
    private Path location;
    private final List<StoragePart> storageParts = new ArrayList<>();

    public boolean init(Path location) throws IOException {
        if (!location.toFile().exists()) {
            return false;
        }

        this.location = location;

        // On every part we write memory file and index file, so devide on 2
        // TODO: walk through files
        storagePartsC = location.toFile().list().length / 2;
        for (int partN = 0; partN < storagePartsC; partN++) {
            addStoragePart(partN);
        }

        return true;
    }

    /**
     * Finds entry with given key in file.
     *
     * @param key - key for entry to find
     * @return entry with the same key or null if there is no entry with the same key
     */
    public BaseEntry<ByteBuffer> findEntry(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> res = null;
        lock.readLock().lock();
        try {
            for (int partN = storagePartsC - 1; partN >= 0; partN--) {
                res = storageParts.get(partN).get(key);
                if (res != null) {
                    break;
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        return res;
    }

    public Iterator<BaseEntry<ByteBuffer>> getMergedEntrys(
            ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> localEntrys, ByteBuffer from, ByteBuffer to) {
        BinaryHeap binaryHeap = new BinaryHeap();
        for (StoragePart storagePart : storageParts) {
            PeekIterator peekIterator = storagePart.get(from, to);
            if (peekIterator.peek() != null) {
                binaryHeap.add(peekIterator);
            }
        }

        PeekIterator localIter = new PeekIterator(localEntrys.values().iterator(), Integer.MAX_VALUE);
        if (localIter.peek() != null) {
            binaryHeap.add(localIter);
        }

        return new StorageSystemIterator(binaryHeap);
    }

    public void save(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> entrys) throws IOException {
        if (entrys.size() == 0) {
            return;
        }

        ByteBuffer bufferForIndexes = ByteBuffer.allocate(entrys.size() * BYTES_IN_LONG);
        lock.writeLock().lock();
        try {
            writeMemFile(entrys, getMemFilePath(storagePartsC), bufferForIndexes);
            writeIndexFile(bufferForIndexes, getIndexFilePath(storagePartsC));
        } finally {
            lock.writeLock().unlock();
        }

        addStoragePart(storagePartsC);
        storagePartsC++;
    }

    @Override
    public void close() {
        for (StoragePart storagePart : storageParts) {
            storagePart.close();
        }
    }

    private void addStoragePart(int partN) throws IOException {
        StoragePart storagePart = new StoragePart();
        storagePart.init(getMemFilePath(partN), getIndexFilePath(partN), partN);
        storageParts.add(storagePart);
    }

    private Path getMemFilePath(int num) {
        return location.resolve(num + MEM_FILENAME);
    }

    private Path getIndexFilePath(int num) {
        return location.resolve(num + INDEX_FILENAME);
    }

    /**
     * Writes entrys in file (memFilePath).
     * Scheme:
     * [key size][key][value size][value]....
     *
     * @param bufferForIndexes in that buffer we write startPos of every entry
     */
    private static void writeMemFile(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> entrys, Path memFileP,
                                     ByteBuffer bufferForIndexes) throws IOException {

        ByteBuffer bufferToWrite = ByteBuffer.allocate(DEFAULT_ALLOC_SIZE);
        long entryStartPos = 0;

        try (
                RandomAccessFile daoMemfile = new RandomAccessFile(memFileP.toFile(), "rw");
                FileChannel memChannel = daoMemfile.getChannel()
        ) {
            for (BaseEntry<ByteBuffer> entry : entrys.values()) {
                bufferForIndexes.putLong(entryStartPos);
                int bytesC = getPersEntryByteSize(entry);
                if (bytesC > bufferToWrite.capacity()) {
                    bufferToWrite = ByteBuffer.allocate(bytesC);
                }

                persistEntry(entry, bufferToWrite);
                memChannel.write(bufferToWrite);
                bufferToWrite.clear();
                entryStartPos += bytesC;
            }
        }
        bufferForIndexes.flip();
    }

    /**
     * Write "start positions of entrys in daoMemFile" in indexFilePath.
     *
     * @param bufferForIndexes buffer with startPos of every entry
     */
    private static void writeIndexFile(ByteBuffer bufferForIndexes, Path indexFileP) throws IOException {
        try (
                RandomAccessFile indexFile = new RandomAccessFile(indexFileP.toFile(), "rw");
                FileChannel indexChannel = indexFile.getChannel()
        ) {
            indexChannel.write(bufferForIndexes);
        }
    }

    /**
     * Saves entry to byteBuffer.
     *
     * @param entry         that we want to save in bufferToWrite
     * @param bufferToWrite buffer where we want to persist entry
     */
    private static void persistEntry(BaseEntry<ByteBuffer> entry, ByteBuffer bufferToWrite) {
        bufferToWrite.putInt(entry.key().array().length);
        bufferToWrite.put(entry.key().array());

        if (entry.value() == null) {
            bufferToWrite.putInt(StoragePart.LEN_FOR_NULL);
        } else {
            bufferToWrite.putInt(entry.value().array().length);
            bufferToWrite.put(entry.value().array());
        }

        bufferToWrite.flip();
    }

    /**
     * Count byte size of entry, that we want to write in file.
     *
     * @param entry that we want to save
     * @return count of bytes
     */
    private static int getPersEntryByteSize(BaseEntry<ByteBuffer> entry) {
        int keyLength = entry.key().array().length;
        int valueLength = entry.value() == null ? 0 : entry.value().array().length;

        return 2 * BYTES_IN_INT + keyLength + valueLength;
    }
}
