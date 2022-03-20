package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> data = new ConcurrentSkipListMap<>();
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    private final Path pathToDataFile;
    private final Path pathToDataIndexes;

    public InMemoryDao(Config config) throws IOException {
        Path pathToConfig = config.basePath();
        pathToDataFile = pathToConfig.resolve("data.txt");
        pathToDataIndexes = pathToConfig.resolve("indexes.txt");

        createFileIfNotExist(pathToDataFile);
        createFileIfNotExist(pathToDataIndexes);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from != null && to != null) {
            return data.subMap(from, to).values().iterator();
        }
        if (from != null) {
            return data.tailMap(from).values().iterator();
        }
        if (to != null) {
            return data.headMap(to).values().iterator();
        }
        return data.values().iterator();
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        try {
            rwlock.readLock().lock();
            BaseEntry<ByteBuffer> result = findKeyInMap(key);
            return result == null ? findKeyInFile(key) : result;
        } finally {
            rwlock.readLock().unlock();
        }
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        try {
            rwlock.readLock().lock();
            data.put(entry.key(), entry);
        } finally {
            rwlock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            rwlock.writeLock().lock();
            write();
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private void createFileIfNotExist(Path filename) throws IOException {
        if (!Files.exists(filename)) {
            Files.createDirectories(filename.getParent());
            Files.createFile(filename);
        }
    }

    private BaseEntry<ByteBuffer> findKeyInMap(ByteBuffer key) {
        return data.get(key);
    }

    private BaseEntry<ByteBuffer> findKeyInFile(ByteBuffer key) throws IOException {
        try (RandomAccessFile dataFile = new RandomAccessFile(pathToDataFile.toString(), "r");
             RandomAccessFile indexesFile = new RandomAccessFile(pathToDataIndexes.toString(), "r")) {
            return binarySearchInFile(key, dataFile, indexesFile);
        }
    }

    private BaseEntry<ByteBuffer> binarySearchInFile(ByteBuffer key, RandomAccessFile dataFile,
                RandomAccessFile indexesFile) throws IOException {
        long a = 0;
        long b = indexesFile.length() / Utils.OFFSET_VALUES_DISTANCE;
        while (b - a >= 1) {
            long c = (b + a) / 2;
            indexesFile.seek(c * Utils.OFFSET_VALUES_DISTANCE);
            int dataFileOffset = indexesFile.readInt();
            dataFile.seek(dataFileOffset);

            ByteBuffer curKey = readByteBuffer(dataFile);
            int compare = curKey.compareTo(key);
            if (compare < 0) {
                a = c;
            } else if (compare == 0) {
                return new BaseEntry<>(curKey, readByteBuffer(dataFile));
            } else {
                b = c;
            }
        }

        return null;
    }

    private ByteBuffer readByteBuffer(RandomAccessFile dataFile) throws IOException {
        int bbSize = dataFile.readInt();
        ByteBuffer bb = ByteBuffer.allocate(bbSize);
        dataFile.getChannel().read(bb);
        bb.rewind();
        return bb;
    }

    private void write() throws IOException {
        if (data.isEmpty()) {
            return;
        }

        try (RandomAccessFile dataFile = new RandomAccessFile(pathToDataFile.toString(), "rw");
             RandomAccessFile indexesFile = new RandomAccessFile(pathToDataIndexes.toString(), "rw")) {
            int curOffset = 0;
            int bbSize = 0;
            ByteBuffer offset = ByteBuffer.allocate(Utils.OFFSET_VALUES_DISTANCE);
            for (BaseEntry<ByteBuffer> entry: data.values()) {
                curOffset += bbSize;
                putOffsetInIndexesFile(curOffset, offset, indexesFile);
                bbSize = putPairInDataFile(entry, dataFile);
            }
        }
    }

    private void putOffsetInIndexesFile(int offset, ByteBuffer bbOffset, RandomAccessFile indexesFile)
            throws IOException {
        bbOffset.putInt(offset);
        bbOffset.putChar(Utils.LINE_SEPARATOR);
        bbOffset.rewind();
        indexesFile.getChannel().write(bbOffset);
        bbOffset.rewind();
    }

    private int putPairInDataFile(BaseEntry<ByteBuffer> entry, RandomAccessFile dataFile) throws IOException {
        int bbSize = Integer.BYTES + entry.key().remaining()
                + Integer.BYTES + entry.value().remaining()
                + Character.BYTES;
        ByteBuffer pair = ByteBuffer.allocate(bbSize);

        pair.putInt(entry.key().remaining());
        pair.put(entry.key());
        pair.putInt(entry.value().remaining());
        pair.put(entry.value());
        pair.putChar(Utils.LINE_SEPARATOR);
        pair.rewind();
        dataFile.getChannel().write(pair);

        return bbSize;
    }

}
