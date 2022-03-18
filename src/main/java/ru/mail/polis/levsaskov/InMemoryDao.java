package ru.mail.polis.levsaskov;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private static final int DEFAULT_ALLOC_SIZE = 2048;
    private static final int BYTES_IN_INT = 4;
    private static final int BYTES_IN_LONG = 8;
    private static final String MEM_FILENAME = "daoMem.bin";
    private static final String INDEX_FILENAME = "daoIndex.bin";

    private Path memFilePath;
    private Path indexFilePath;

    private final ConcurrentSkipListMap<ByteBuffer, BaseEntry<ByteBuffer>> entrys = new ConcurrentSkipListMap<>();

    public InMemoryDao() {
        // This constructor is intentionally empty. Nothing special is needed here.
    }

    public InMemoryDao(Config config) {
        this.memFilePath = config.basePath().resolve(MEM_FILENAME);
        this.indexFilePath = config.basePath().resolve(INDEX_FILENAME);
    }

    @Override
    public BaseEntry<ByteBuffer> get(@NotNull ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> localVal = entrys.get(key);

        return localVal == null ? findEntryInFileSystem(key, memFilePath, indexFilePath) : localVal;
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        Iterator<BaseEntry<ByteBuffer>> ans;

        if (from == null && to == null) {
            ans = entrys.values().iterator();
        } else if (from == null) {
            ans = entrys.headMap(to).values().iterator();
        } else if (to == null) {
            ans = entrys.tailMap(from).values().iterator();
        } else {
            ans = entrys.subMap(from, to).values().iterator();
        }

        return ans;
    }

    @Override
    public void upsert(@NotNull BaseEntry<ByteBuffer> entry) {
        entrys.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        ByteBuffer bufferForIndexes = ByteBuffer.allocate(entrys.size() * BYTES_IN_LONG);
        writeMemFile(bufferForIndexes);
        writeIndexFile(bufferForIndexes);
    }

    /**
     * Writes entrys in file (memFilePath).
     * Scheme:
     * [key size][key][value size][value]....
     *
     * @param bufferForIndexes in that buffer we write startPos of every entry
     */
    private void writeMemFile(ByteBuffer bufferForIndexes) throws IOException {
        try (
                RandomAccessFile daoMemfile = new RandomAccessFile(memFilePath.toFile(), "rw");
                FileChannel memChannel = daoMemfile.getChannel()
        ) {
            ByteBuffer bufferToWrite = ByteBuffer.allocate(DEFAULT_ALLOC_SIZE);
            long entryStartPos = 0;

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
    private void writeIndexFile(ByteBuffer bufferForIndexes) throws IOException {
        try (
                RandomAccessFile indexFile = new RandomAccessFile(indexFilePath.toFile(), "rw");
                FileChannel indexChannel = indexFile.getChannel()
        ) {
            indexChannel.write(bufferForIndexes);
        }
    }

    private static ByteBuffer readIndexFile(Path indexFileP) throws IOException {
        ByteBuffer bufferForIndexes;
        try (
                RandomAccessFile indexFile = new RandomAccessFile(indexFileP.toFile(), "rw");
                FileChannel indexChannel = indexFile.getChannel();
        ) {
            bufferForIndexes = ByteBuffer.allocate((int) indexChannel.size());
            indexChannel.read(bufferForIndexes);
        }

        return bufferForIndexes;
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

    /**
     * Finds entry with given key in file.
     *
     * @param memFileP - path of file to find in
     * @param key      - key for entry to find
     * @return entry with the same key or null if there is no entry with the same key
     */
    private static BaseEntry<ByteBuffer> findEntryInFileSystem(ByteBuffer key, Path memFileP,
                                                               Path indexFileP) throws IOException {
        if (memFileP == null || !memFileP.toFile().exists()
                || indexFileP == null || !indexFileP.toFile().exists()) {
            return null;
        }

        BaseEntry<ByteBuffer> res;
        ByteBuffer bufferForIndexes = readIndexFile(indexFileP);
        try (
                RandomAccessFile daoMemfile = new RandomAccessFile(memFileP.toFile(), "rw");
                FileChannel memChannel = daoMemfile.getChannel()

        ) {
            int entrysC = bufferForIndexes.capacity() / BYTES_IN_LONG;
            res = binarySearch(bufferForIndexes, memChannel, entrysC, key);
        }

        return res;
    }

    private static BaseEntry<ByteBuffer> binarySearch(ByteBuffer bufferForIndexes, FileChannel memChannel,
                                                      int inLast, ByteBuffer key) throws IOException {
        int first = 0;
        int last = inLast;
        int position = (first + last) / 2;
        BaseEntry<ByteBuffer> curEntry = getEntry(bufferForIndexes, memChannel, position);

        while ((!curEntry.key().equals(key))
                && (first <= last)) {
            if (curEntry.key().compareTo(key) > 0) {
                last = position - 1;
            } else {
                first = position + 1;
            }
            position = (first + last) / 2;
            curEntry = getEntry(bufferForIndexes, memChannel, position);
        }
        return curEntry.key().equals(key) ? curEntry : null;
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
