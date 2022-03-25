package ru.mail.polis.kirillpobedonostsev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistenceDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    public static final int NULL_VALUE_LENGTH = -1;
    private static final String ELEMENTS_FILENAME = "elements.dat";
    private static final String DATA_PREFIX = "data";
    private static final String INDEX_PREFIX = "index";
    private final Path elementsPath;
    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map =
            new ConcurrentSkipListMap<>(ByteBuffer::compareTo);
    private final List<FilePair> filesList;
    private final Config config;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public PersistenceDao(Config config) throws IOException {
        this.config = config;
        this.elementsPath = config.basePath().resolve(ELEMENTS_FILENAME);
        int fileNumber;
        try (RandomAccessFile file = new RandomAccessFile(elementsPath.toFile(), "r")) {
            fileNumber = file.readInt();
        } catch (FileNotFoundException e) {
            fileNumber = 0;
        }
        filesList = new ArrayList<>(fileNumber);
        for (int i = 0; i < fileNumber; i++) {
            try (FileChannel dataChannel = FileChannel.open(getFilePath(DATA_PREFIX, i));
                 FileChannel indexChannel = FileChannel.open(getFilePath(INDEX_PREFIX, i))) {
                MappedByteBuffer mappedDataFile =
                        dataChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataChannel.size());
                MappedByteBuffer mappedIndexFile =
                        indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
                filesList.add(new FilePair(mappedIndexFile, mappedDataFile));
            }
        }
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        lock.readLock().lock();
        try {
            List<PeekingIterator<BaseEntry<ByteBuffer>>> iteratorsList =
                    new ArrayList<>(filesList.size() + 1);
            iteratorsList.add(new PeekingIterator<>(getInMemoryIterator(from, to), filesList.size()));
            for (int i = filesList.size() - 1; i >= 0; i--) {
                iteratorsList.add(new PeekingIterator<>(getFileIterator(from, to, i), i));
            }
            return new MergeIterator(iteratorsList);
        } finally {
            lock.readLock().unlock();
        }
    }

    private FileIterator getFileIterator(ByteBuffer from, ByteBuffer to, int fileNumber) {
        ByteBuffer fileRange;
        MappedByteBuffer mappedDataFile = filesList.get(fileNumber).dataFile();
        MappedByteBuffer mappedIndexFile = filesList.get(fileNumber).indexFile();
        int fromOffset = from == null ? 0 : getOffset(mappedDataFile, mappedIndexFile, from);
        int toOffset =
                to == null ? mappedDataFile.capacity() : getOffset(mappedDataFile, mappedIndexFile, to);
        fileRange = mappedDataFile.slice(fromOffset, toOffset - fromOffset);
        return new FileIterator(fileRange);
    }

    private Iterator<BaseEntry<ByteBuffer>> getInMemoryIterator(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return map.values().iterator();
        } else if (from == null) {
            return map.headMap(to, false).values().iterator();
        } else if (to == null) {
            return map.tailMap(from, true).values().iterator();
        }
        return map.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        lock.readLock().lock();
        try {
            map.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (map.isEmpty()) {
                return;
            }
            long size = 0;
            for (Entry<ByteBuffer> entry : map.values()) {
                size += entry.key().remaining();
                if (entry.value() != null) {
                    size += entry.value().remaining();
                }
            }
            size += map.size() * 2L * Integer.BYTES;

            MappedByteBuffer dataPage;
            MappedByteBuffer indexPage;
            try (FileChannel dataChannel = FileChannel.open(getFilePath(DATA_PREFIX, filesList.size()),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
                 FileChannel indexChannel = FileChannel.open(getFilePath(INDEX_PREFIX, filesList.size()),
                         StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
                dataPage = dataChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
                indexPage = indexChannel.map(FileChannel.MapMode.READ_WRITE, 0,
                        (long) map.size() * Integer.BYTES);
            }
            for (Entry<ByteBuffer> entry : map.values()) {
                indexPage.putInt(dataPage.position());
                dataPage.putInt(entry.key().remaining());
                dataPage.put(entry.key());
                if (entry.value() == null) {
                    dataPage.putInt(NULL_VALUE_LENGTH);
                } else {
                    dataPage.putInt(entry.value().remaining());
                    dataPage.put(entry.value());
                }
            }
            dataPage.force();
            indexPage.force();
            filesList.add(new FilePair(indexPage, dataPage));
            try (RandomAccessFile file = new RandomAccessFile(elementsPath.toFile(), "rw")) {
                file.writeInt(filesList.size());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Path getFilePath(String prefix, int number) {
        return config.basePath().resolve(prefix + number + ".dat");
    }

    private static int getOffset(MappedByteBuffer readDataPage, MappedByteBuffer readIndexPage,
                                 ByteBuffer key) {
        int indexNumber = binarySearch(readDataPage, readIndexPage, key);
        int indexOffset = indexNumber << 2;
        if (readIndexPage.capacity() <= indexOffset) {
            return readDataPage.capacity();
        }
        return readIndexPage.getInt(indexOffset);
    }

    private static int binarySearch(MappedByteBuffer readDataPage, MappedByteBuffer readIndexPage,
                                    ByteBuffer key) {
        int low = 0;
        int high = readIndexPage.capacity() / Integer.BYTES - 1;
        int offset;
        while (low <= high) {
            int mid = (int) (((long) low + high) / 2);
            offset = readIndexPage.getInt(mid << 2);
            int keySize = readDataPage.getInt(offset);
            ByteBuffer readKey = readDataPage.slice(offset + Integer.BYTES, keySize);
            int compareResult = readKey.compareTo(key);
            if (compareResult > 0) {
                high = mid - 1;
            } else if (compareResult < 0) {
                low = mid + 1;
            } else {
                return mid;
            }
        }
        return low;
    }
}
