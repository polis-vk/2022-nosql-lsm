package ru.mail.polis.levsaskov;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageSystem {
    private static final int DEFAULT_ALLOC_SIZE = 2048;
    private static final int BYTES_IN_INT = 4;
    private static final int BYTES_IN_LONG = 8;
    private static final String MEM_FILENAME = "daoMem.bin";
    private static final String INDEX_FILENAME = "daoIndex.bin";

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private int dataBasePartsC;
    private Path location;

    public boolean init(Path location) {
        if (!location.toFile().exists()) {
            return false;
        }

        this.location = location;
        //On every part we write memory file and index file, so devide on 2
        dataBasePartsC = location.toFile().list().length / 2;
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
            for (int partN = dataBasePartsC - 1; partN >= 0; partN--) {
                ByteBuffer bufferForIndexes = readIndexFile(getIndexFilePath(partN));
                res = searchInMemFile(key, bufferForIndexes, getMemFilePath(partN));
                if (res != null) {
                    break;
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        return res;
    }

    public ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> getMergedEntrys(
            ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> localEntrys, ByteBuffer from, ByteBuffer to)
            throws IOException {
        ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> res = new ConcurrentSkipListMap<>();
        lock.readLock().lock();
        try {
            for (int partN = 0; partN < dataBasePartsC; partN++) {
                ByteBuffer bufferForIndexes = readIndexFile(getIndexFilePath(partN));
                res.putAll(getRangeInMemFile(from, to, bufferForIndexes, getMemFilePath(partN)));
            }
        } finally {
            lock.readLock().unlock();
        }

        if (res.size() == 0) {
            return localEntrys;
        }

        res.putAll(localEntrys);
        return res;
    }

    public void save(ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> entrys) throws IOException {
        ByteBuffer bufferForIndexes = ByteBuffer.allocate(entrys.size() * BYTES_IN_LONG);
        lock.writeLock().lock();
        try {
            writeMemFile(entrys, getMemFilePath(dataBasePartsC), bufferForIndexes);
            writeIndexFile(bufferForIndexes, getIndexFilePath(dataBasePartsC));
        } finally {
            lock.writeLock().unlock();
        }

        dataBasePartsC++;
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
                // Возможно переполнение, нужно облагородить к следующему stage
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
     * Saves indexes in ByteBuffer.
     *
     * @param indexFileP path of index file
     * @return ByteBuffer with indexes
     */
    private static ByteBuffer readIndexFile(Path indexFileP) throws IOException {
        ByteBuffer bufferForIndexes;
        try (
                RandomAccessFile indexFile = new RandomAccessFile(indexFileP.toFile(), "rw");
                FileChannel indexChannel = indexFile.getChannel()
        ) {
            bufferForIndexes = ByteBuffer.allocate((int) indexChannel.size());
            indexChannel.read(bufferForIndexes);
        }

        return bufferForIndexes;
    }

    /**
     * Find range of base entrys in memFile of keys from "from" to "to".
     *
     * @param bufferForIndexes buffer with indexes of memFile
     * @param memFileP         path of memFile
     * @return map with range
     */
    private static NavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> getRangeInMemFile(
            ByteBuffer from, ByteBuffer to, ByteBuffer bufferForIndexes, Path memFileP) throws IOException {
        NavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> res = new TreeMap<>();
        try (
                RandomAccessFile daoMemfile = new RandomAccessFile(memFileP.toFile(), "rw");
                FileChannel memChannel = daoMemfile.getChannel()

        ) {
            int entrysC = bufferForIndexes.capacity() / BYTES_IN_LONG;

            int pos = binarySearch(bufferForIndexes, memChannel, entrysC, from);
            BaseEntry<ByteBuffer> entry;
            if (from == null || getEntry(bufferForIndexes, memChannel, pos).key().compareTo(from) >= 0) {
                while (pos < entrysC) {
                    entry = getEntry(bufferForIndexes, memChannel, pos);
                    if (to != null && entry.key().compareTo(to) >= 0) {
                        break;
                    }

                    res.put(entry.key(), entry);
                    pos++;
                }
            }
        }

        return res;
    }

    /**
     * Search base entry with given key in memory file.
     *
     * @param bufferForIndexes buffer with indexes of memFile
     * @param memFileP         path of memFile
     * @return base entry with given key or null if there is no such key in file
     */
    private static BaseEntry<ByteBuffer> searchInMemFile(ByteBuffer key, ByteBuffer bufferForIndexes, Path memFileP) throws IOException {
        BaseEntry<ByteBuffer> res;
        try (
                RandomAccessFile daoMemfile = new RandomAccessFile(memFileP.toFile(), "rw");
                FileChannel memChannel = daoMemfile.getChannel()

        ) {
            int entrysC = bufferForIndexes.capacity() / BYTES_IN_LONG;
            int position = binarySearch(bufferForIndexes, memChannel, entrysC, key);
            res = getEntry(bufferForIndexes, memChannel, position);
        }

        return res.key().equals(key) ? res : null;
    }

    /**
     * Count byte size of entry, that we want to write in file.
     *
     * @param entry that we want to save
     * @return count of bytes
     */
    private static int getPersEntryByteSize(BaseEntry<ByteBuffer> entry) {
        int keyLength = entry.key().array().length;
        int valueLength = entry.value().array().length;

        return 2 * BYTES_IN_INT + keyLength + valueLength;
    }

    private static int binarySearch(ByteBuffer bufferForIndexes, FileChannel memChannel,
                                    int inLast, ByteBuffer key) throws IOException {
        if (key == null) {
            return 0;
        }

        int first = 0;
        int last = inLast;
        int position = (first + last) / 2;
        BaseEntry<ByteBuffer> curEntry = getEntry(bufferForIndexes, memChannel, position);

        while (!curEntry.key().equals(key) && first <= last) {
            if (curEntry.key().compareTo(key) > 0) {
                last = position - 1;
            } else {
                first = position + 1;
            }
            position = (first + last) / 2;
            curEntry = getEntry(bufferForIndexes, memChannel, position);
        }
        return position;
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

        bufferToWrite.putInt(entry.value().array().length);
        bufferToWrite.put(entry.value().array());
        bufferToWrite.flip();
    }

    private static BaseEntry<ByteBuffer> convertToEntry(ByteBuffer byteBuffer) {
        int keyLen = byteBuffer.getInt();
        byte[] key = new byte[keyLen];
        byteBuffer.get(key);

        int valueLen = byteBuffer.getInt();
        byte[] value = new byte[valueLen];
        byteBuffer.get(value);

        return new BaseEntry<>(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
    }

    private static BaseEntry<ByteBuffer> getEntry(ByteBuffer bufferForIndexes, FileChannel memChannel,
                                                  int entryNum) throws IOException {
        long ind = bufferForIndexes.getLong(entryNum * BYTES_IN_LONG);

        long bytesCount;
        if (entryNum == bufferForIndexes.capacity() / BYTES_IN_LONG - 1) {
            bytesCount = memChannel.size() - ind;
        } else {
            bytesCount = bufferForIndexes.getLong((entryNum + 1) * BYTES_IN_LONG) - ind;
        }

        ByteBuffer entryByteBuffer = ByteBuffer.allocate((int) bytesCount);
        memChannel.read(entryByteBuffer, ind);
        entryByteBuffer.flip();
        return convertToEntry(entryByteBuffer);
    }

}
