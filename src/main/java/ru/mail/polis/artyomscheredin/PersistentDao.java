package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.file.Files.readAllBytes;
import static ru.mail.polis.artyomscheredin.Utils.readEntry;

public class PersistentDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private static final String DATA_FILE_NAME = "data";
    private static final String INDEXES_FILE_NAME = "indexes";
    private static final String META_INFO_FILE_NAME = "meta";
    private static final String EXTENSION = ".txt";

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> inMemoryData =
            new ConcurrentSkipListMap<>(ByteBuffer::compareTo);
    private final List<Utils.mappedBufferedPair> mappedDiskData;
    private final Config config;

    public PersistentDao(Config config) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException();
        }
        this.config = config;
        mappedDiskData = mapDiskData();
    }

    private List<Utils.mappedBufferedPair> mapDiskData() throws IOException {
        int index = readPrevIndex();
        List<Utils.mappedBufferedPair> list = new LinkedList<>();
        for (int i = 1; i <= index; i++) {
            try (FileChannel dataChannel = FileChannel.open(config.basePath().resolve(DATA_FILE_NAME + i + EXTENSION));
                 FileChannel indexChannel = FileChannel.open(config.basePath().resolve(INDEXES_FILE_NAME + i + EXTENSION))) {
                ByteBuffer indexBuffer = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
                ByteBuffer dataBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataChannel.size());
                list.add(new Utils.mappedBufferedPair(dataBuffer, indexBuffer));
            }
        }
        return list;
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        lock.readLock().lock();
        try {
            if (mappedDiskData.isEmpty()) {
                return getInMemoryIterator(from, to);
            }
            List<PeekIterator> iteratorsList = new ArrayList<>();

            for (Utils.mappedBufferedPair pair : mappedDiskData) {
                iteratorsList.add(new PeekIterator(new FileIterator(pair.getDataBuffer().rewind(),
                        pair.getIndexBuffer().rewind(), from, to)));
            }
            if (!inMemoryData.isEmpty()) {
                iteratorsList.add(new PeekIterator(getInMemoryIterator(from, to)));
            }
            return new MergeIterator(iteratorsList);
        } finally {
            lock.readLock().unlock();
        }
    }

    private Iterator<BaseEntry<ByteBuffer>> getInMemoryIterator(ByteBuffer from, ByteBuffer to) {
        if ((from == null) && (to == null)) {
            return inMemoryData.values().iterator();
        } else if (from == null) {
            return inMemoryData.headMap(to).values().iterator();
        } else if (to == null) {
            return inMemoryData.tailMap(from).values().iterator();
        } else {
            return inMemoryData.subMap(from, to).values().iterator();
        }
    }
/*

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        lock.readLock().lock();
        try {
            BaseEntry<ByteBuffer> value = inMemoryData.get(key);
            if (value != null) {
                return value;
            }

            List<Utils.PathPair> paths = getDataPathsToRead();

            Collections.reverse(paths);
            for (Utils.PathPair path : paths) {
                BaseEntry<ByteBuffer> entry = getEntryIfExists(path, key);
                if (entry != null) {
                    return entry;
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }
*/


    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        if (entry == null) {
            throw new IllegalArgumentException();
        }
        lock.readLock().lock();
        try {
            inMemoryData.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            store(inMemoryData);
            inMemoryData.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /*private BaseEntry<ByteBuffer> getEntryIfExists(Utils.mappedBufferedPair paths, ByteBuffer key) throws IOException {
        ByteBuffer indexBuffer;
        ByteBuffer dataBuffer;
        try (FileChannel dataChannel = FileChannel.open(paths.getDataPath());
             FileChannel indexChannel = FileChannel.open(paths.getIndexPath())) {
            indexBuffer = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
            dataBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataChannel.size());
        } catch (NoSuchFileException e) {
            return null;
        }

        int offset = findOffset(indexBuffer, dataBuffer, key);
        if (offset == -1) {
            return null;
        }

        return readEntry(dataBuffer, offset);
    }*/
/*
    private int findOffset(ByteBuffer indexBuffer, ByteBuffer dataBuffer, ByteBuffer key) {
        int offset = 0;
        int low = 0;
        int high = indexBuffer.remaining() / Integer.BYTES - 1;
        while (low <= high) {
            int mid = low + ((high - low) / 2);
            offset = indexBuffer.getInt(mid * Integer.BYTES);
            int keySize = dataBuffer.getInt(offset);

            ByteBuffer curKey = dataBuffer.slice(offset + Integer.BYTES, keySize);
            if (curKey.compareTo(key) < 0) {
                low = mid + 1;
            } else if (curKey.compareTo(key) > 0) {
                high = mid - 1;
            } else if (curKey.compareTo(key) == 0) {
                return offset;
            }
        }
        indexBuffer.rewind();
        dataBuffer.rewind();
        return -1;
    }*/

    private int readPrevIndex() throws IOException {
        Path pathToReadMetaInfo = config.basePath().resolve(META_INFO_FILE_NAME + EXTENSION);
        try {
            ByteBuffer temp = ByteBuffer.wrap(readAllBytes(pathToReadMetaInfo));
            temp.rewind();
            return temp.getInt();
        } catch (NoSuchFileException e) {
            return 0;
        }
    }

    private void store(SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> data) throws IOException {
        if (data == null) {
            return;
        }
        int index = readPrevIndex() + 1;

        Path pathToWriteData = config.basePath().resolve(DATA_FILE_NAME + index + EXTENSION);
        Path pathToWriteIndexes = config.basePath().resolve(INDEXES_FILE_NAME + index + EXTENSION);

        try (FileChannel dataChannel = FileChannel.open(pathToWriteData,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
             FileChannel indexChannel = FileChannel.open(pathToWriteIndexes,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.TRUNCATE_EXISTING)) {

            int size = 0;
            for (Entry<ByteBuffer> el : inMemoryData.values()) {
                size += el.key().remaining() + el.value().remaining();
            }
            size += 2 * inMemoryData.size() * Integer.BYTES;

            ByteBuffer writeDataBuffer = dataChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
            ByteBuffer writeIndexBuffer = indexChannel.map(FileChannel.MapMode.READ_WRITE, 0,
                    (long) inMemoryData.size() * Integer.BYTES);

            for (Entry<ByteBuffer> el : inMemoryData.values()) {
                writeIndexBuffer.putInt(writeDataBuffer.position());

                writeDataBuffer.putInt(el.key().remaining());
                writeDataBuffer.put(el.key());
                writeDataBuffer.putInt(el.value().remaining());
                writeDataBuffer.put(el.value());
            }
        }

        try (RandomAccessFile file = new RandomAccessFile(config.basePath()
                .resolve(META_INFO_FILE_NAME + EXTENSION).toFile(), "rw")) {
            file.writeInt(index);
        }
    }
}
