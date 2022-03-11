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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistenceDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private static final String ELEMENTS_FILENAME = "elements.dat";
    private static final String DATA_PREFIX = "data";
    private static final String INDEX_PREFIX = "index";

    private int fileNumber;
    private final Path elementsPath;
    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map =
            new ConcurrentSkipListMap<>(ByteBuffer::compareTo);

    private final Config config;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

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
            List<Iterator<BaseEntry<ByteBuffer>>> iteratorsList = new ArrayList<>(fileNumber + 1);
            iteratorsList.add(getInMemoryIterator(from, to));
            for (int i = fileNumber - 1; i >= 0; i--) {
                iteratorsList.add(
                        getIteratorFromFile(from, to, getFilePath(DATA_PREFIX, i), getFilePath(INDEX_PREFIX, i)));
            }
            return new MergeIterator(iteratorsList);
        } finally {
            lock.readLock().unlock();
        }
    }

    private FileIterator getIteratorFromFile(ByteBuffer from, ByteBuffer to, Path dataPath, Path indexPath)
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

    private int floorOffset(MappedByteBuffer readDataPage, MappedByteBuffer readIndexPage, ByteBuffer to) {
        int low = 0;
        int high = readIndexPage.capacity() / Integer.BYTES - 1;
        boolean isLower = false;
        int offset = 0;
        int mid;
        while (low <= high) {
            mid = (high + low) / 2;
            readIndexPage.position(mid * Integer.BYTES);
            offset = readIndexPage.getInt();
            readDataPage.position(offset);
            int keySize = readDataPage.getInt();
            if (keySize != to.remaining()) {
                continue;
            }
            ByteBuffer readKey = readDataPage.slice(readDataPage.position(), keySize);
            int compareResult = readKey.compareTo(to);
            if (compareResult > 0) {
                high = mid - 1;
                isLower = false;
            } else if (compareResult < 0) {
                low = mid + 1;
                isLower = true;
            } else {
                high = mid - 1;
                isLower = false;
            }
        }
        if (isLower) {
            readIndexPage.position(high * Integer.BYTES);
            offset = readIndexPage.getInt();
        }
        return offset;
    }

    private int ceilOffset(MappedByteBuffer readDataPage, MappedByteBuffer readIndexPage, ByteBuffer from) {
        int low = 0;
        int high = readIndexPage.capacity() / Integer.BYTES - 1;
        boolean isLower = false;
        int offset = 0;
        int mid = 0;
        while (low <= high) {
            mid = (high + low) / 2;
            readIndexPage.position(mid * Integer.BYTES);
            offset = readIndexPage.getInt();
            readDataPage.position(offset);
            int keySize = readDataPage.getInt();
            if (keySize != from.remaining()) {
                continue;
            }
            ByteBuffer readKey = readDataPage.slice(readDataPage.position(), keySize);
            int compareResult = readKey.compareTo(from);
            if (compareResult > 0) {
                high = mid - 1;
                isLower = false;
            } else if (compareResult < 0) {
                low = mid + 1;
                isLower = true;
            } else {
                return offset;
            }
        }
        if (!isLower) {
            readIndexPage.position(mid * Integer.BYTES);
            offset = readIndexPage.getInt();
        }
        return offset;
    }

    private Iterator<BaseEntry<ByteBuffer>> getInMemoryIterator(ByteBuffer from, ByteBuffer to) {
        Iterator<BaseEntry<ByteBuffer>> inMemoryIter;
        if (from == null && to == null) {
            inMemoryIter = map.values().iterator();
        } else if (from == null) {
            inMemoryIter = map.headMap(to, false).values().iterator();
        } else if (to == null) {
            inMemoryIter = map.tailMap(from, true).values().iterator();
        } else {
            inMemoryIter = map.subMap(from, true, to, false).values().iterator();
        }
        return inMemoryIter;
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

    private ByteBuffer searchInFile(MappedByteBuffer readDataPage, MappedByteBuffer readIndexPage, ByteBuffer key) {
        int low = 0;
        int high = readIndexPage.capacity() / Integer.BYTES - 1;

        while (low <= high) {
            int mid = (high + low) / 2;
            readIndexPage.position(mid * Integer.BYTES);
            int offset = readIndexPage.getInt();
            readDataPage.position(offset);
            int keySize = readDataPage.getInt();
            if (keySize != key.remaining()) {
                continue;
            }
            ByteBuffer readKey = readDataPage.slice(readDataPage.position(), keySize);
            int compareResult = readKey.compareTo(key);
            if (compareResult > 0) {
                high = mid - 1;
            } else if (compareResult < 0) {
                low = mid + 1;
            } else {
                readDataPage.position(readDataPage.position() + keySize);
                int valueSize = readDataPage.getInt();
                ByteBuffer value = readDataPage.slice(readDataPage.position(), valueSize);
                readDataPage.position(readKey.position() + valueSize);
                return value;
            }
        }
        return null;
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

    private static class FileIterator implements Iterator<BaseEntry<ByteBuffer>> {
        private final ByteBuffer mappedFile;

        public FileIterator(ByteBuffer mappedFile) {
            this.mappedFile = mappedFile;
        }

        @Override
        public boolean hasNext() {
            return mappedFile.remaining() != 0;
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            int keySize = mappedFile.getInt();
            ByteBuffer key = mappedFile.slice(mappedFile.position(), keySize);
            mappedFile.position(mappedFile.position() + keySize);
            int valueSize = mappedFile.getInt();
            ByteBuffer value = mappedFile.slice(mappedFile.position(), valueSize);
            mappedFile.position(mappedFile.position() + valueSize);
            return new BaseEntry<>(key, value);
        }
    }

    private static class MergeIterator implements Iterator<BaseEntry<ByteBuffer>> {

        private final Map<Iterator<BaseEntry<ByteBuffer>>, BaseEntry<ByteBuffer>> curMap;
        private final TreeSet<Iterator<BaseEntry<ByteBuffer>>> iteratorsSet;

        public MergeIterator(List<Iterator<BaseEntry<ByteBuffer>>> iterators) {
            this.curMap = new HashMap<>();
            this.iteratorsSet = new TreeSet<>(Comparator.comparing(i -> this.curMap.get(i).key()));
            for (Iterator<BaseEntry<ByteBuffer>> iter : iterators) {
                if (iter.hasNext()) {
                    curMap.put(iter, iter.next());
                    this.iteratorsSet.add(iter);
                }
            }
        }

        @Override
        public boolean hasNext() {
            return !iteratorsSet.isEmpty();
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Iterator<BaseEntry<ByteBuffer>> iter = iteratorsSet.pollFirst();
            BaseEntry<ByteBuffer> res = curMap.get(iter);
            if (iter != null && iter.hasNext()) {
                curMap.put(iter, iter.next());
                iteratorsSet.add(iter);
            }
            return res;
        }
    }
}
