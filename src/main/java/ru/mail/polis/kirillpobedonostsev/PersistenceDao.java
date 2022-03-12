package ru.mail.polis.kirillpobedonostsev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import static ru.mail.polis.kirillpobedonostsev.SearchUtility.ceilOffset;
import static ru.mail.polis.kirillpobedonostsev.SearchUtility.floorOffset;
import static ru.mail.polis.kirillpobedonostsev.SearchUtility.searchInFile;

public class PersistenceDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private static final String ELEMENTS_FILENAME = "elements.dat";
    private static final String DATA_PREFIX = "data";
    private static final String INDEX_PREFIX = "index";
    private final Path elementsPath;
    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map =
            new ConcurrentSkipListMap<>(ByteBuffer::compareTo);
    private final Config config;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile int fileNumber;

    public PersistenceDao(Config config) throws IOException {
        this.config = config;
        this.elementsPath = config.basePath().resolve(ELEMENTS_FILENAME);
        if (Files.notExists(config.basePath())) {
            Files.createDirectories(config.basePath());
        }
        try (RandomAccessFile file = new RandomAccessFile(elementsPath.toFile(), "rw")) {
            fileNumber = file.readInt();
        } catch (EOFException e) {
            fileNumber = 0;
        }
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        lock.readLock().lock();
        try {
            Iterator<BaseEntry<ByteBuffer>> inMemoryIterator = getInMemoryIterator(from, to);
            if (fileNumber == 0) {
                return inMemoryIterator;
            }
            List<Iterator<BaseEntry<ByteBuffer>>> iteratorsList = new ArrayList<>(fileNumber + 1);
            iteratorsList.add(inMemoryIterator);
            for (int i = fileNumber - 1; i >= 0; i--) {
                iteratorsList.add(getFileIterator(from, to, getFilePath(DATA_PREFIX, i), getFilePath(INDEX_PREFIX, i)));
            }
            return new MergeIterator(iteratorsList);
        } finally {
            lock.readLock().unlock();
        }
    }

    private FileIterator getFileIterator(ByteBuffer from, ByteBuffer to, Path dataPath, Path indexPath)
            throws IOException {
        MappedByteBuffer fileRange;
        try (FileChannel dataChannel = FileChannel.open(dataPath);
             FileChannel indexChannel = FileChannel.open(indexPath)) {
            MappedByteBuffer mappedDataFile = dataChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataChannel.size());
            MappedByteBuffer mappedIndexFile = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
            int fromOffset = from == null ? 0 : ceilOffset(mappedDataFile, mappedIndexFile, from);
            int toOffset = to == null ? (int) dataChannel.size() : floorOffset(mappedDataFile, mappedIndexFile, to);
            fileRange = dataChannel.map(FileChannel.MapMode.READ_ONLY, fromOffset, toOffset - fromOffset);
        }
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
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        lock.readLock().lock();
        try {
            BaseEntry<ByteBuffer> entry = map.get(key);
            if (entry != null) {
                return entry;
            }
            if (fileNumber == 0) {
                return null;
            }
            ByteBuffer value = null;
            int number = fileNumber - 1;
            while (value == null && number >= 0) {
                MappedByteBuffer dataPage;
                MappedByteBuffer indexPage;
                try (FileChannel dataChannel = FileChannel.open(getFilePath(DATA_PREFIX, number));
                     FileChannel indexChannel = FileChannel.open(getFilePath(INDEX_PREFIX, number))) {
                    dataPage = dataChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataChannel.size());
                    indexPage = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
                }
                value = searchInFile(dataPage, indexPage, key);
                number--;
            }
            return value == null ? null : new BaseEntry<>(key, value);
        } finally {
            lock.readLock().unlock();
        }
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
            for (Entry<ByteBuffer> value : map.values()) {
                size += value.value().remaining() + value.key().remaining();
            }
            size += map.size() * 2L * Integer.BYTES;

            MappedByteBuffer dataPage;
            MappedByteBuffer indexPage;
            try (FileChannel dataChannel = FileChannel.open(getFilePath(DATA_PREFIX, fileNumber),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ,
                    StandardOpenOption.TRUNCATE_EXISTING);
                 FileChannel indexChannel = FileChannel.open(getFilePath(INDEX_PREFIX, fileNumber),
                         StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ,
                         StandardOpenOption.TRUNCATE_EXISTING)) {
                dataPage = dataChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
                indexPage = indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, (long) map.size() * Integer.BYTES);
            }
            for (Entry<ByteBuffer> entry : map.values()) {
                indexPage.putInt(dataPage.position());
                dataPage.putInt(entry.key().remaining());
                dataPage.put(entry.key());
                dataPage.putInt(entry.value().remaining());
                dataPage.put(entry.value());
            }
            fileNumber++;
            try (RandomAccessFile file = new RandomAccessFile(elementsPath.toFile(), "rw")) {
                file.writeInt(fileNumber);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Path getFilePath(String prefix, int number) {
        return config.basePath().resolve(prefix + number + ".dat");
    }
}
