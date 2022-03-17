package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static ru.mail.polis.artyomscheredin.Utils.readEntry;

public class PersistentDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private static final String DATA_FILE_NAME = "data";
    private static final String INDEXES_FILE_NAME = "indexes";
    private static final String META_INFO_FILE_NAME = "meta";
    private static final String EXTENSION = ".txt";

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> inMemoryData =
            new ConcurrentSkipListMap<>(ByteBuffer::compareTo);
    private final Config config;

    public PersistentDao(Config config) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException();
        }
        this.config = config;
        readPrevIndex();
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        lock.readLock().lock();
        try {
            List<Iterator<BaseEntry<ByteBuffer>>> iteratorsList = new ArrayList<>();

            List<Utils.Pair<Path>> paths = getDataPathsToRead();
            for (Utils.Pair<Path> path : paths) {
                iteratorsList.add(new FileIterator(path, from, to));
            }
            if (!inMemoryData.isEmpty()) {
                iteratorsList.add(getInMemoryIterator(from, to));
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
        }
        return inMemoryData.subMap(from, to).values().iterator();
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        lock.readLock().lock();
        try {
            BaseEntry<ByteBuffer> value = inMemoryData.get(key);
            if (value != null) {
                return value;
            }

            List<Utils.Pair<Path>> paths = getDataPathsToRead();

            Collections.reverse(paths);
            for (Utils.Pair<Path> path : paths) {
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

    private List<Utils.Pair<Path>> getDataPathsToRead() throws IOException {
        int index = readPrevIndex();
        List<Utils.Pair<Path>> list = new LinkedList<>();
        for (int i = 1; i <= index; i++) {
            Utils.Pair<Path> curPaths = new Utils.Pair<Path>(config.basePath().resolve(DATA_FILE_NAME + i + EXTENSION),
                    config.basePath().resolve(INDEXES_FILE_NAME + i + EXTENSION));
            list.add(curPaths);
        }
        return list;
    }

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

    private BaseEntry<ByteBuffer> getEntryIfExists(Utils.Pair<Path> paths, ByteBuffer key) throws IOException {
        ByteBuffer indexBuffer;
        ByteBuffer dataBuffer;
        try (FileChannel dataChannel = FileChannel.open(paths.dataPath());
             FileChannel indexChannel = FileChannel.open(paths.indexPath())) {
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

/*
        int offset = 0;
        while (dataBuffer.remaining() > 0) {
            int keySize = dataBuffer.getInt();
            offset += Integer.BYTES;

            if (keySize != key.remaining()) {
                dataBuffer.position(offset + keySize);
                int valueSize = dataBuffer.getInt();
                dataBuffer.position(offset + valueSize);
                continue;
            }

            ByteBuffer curKey = dataBuffer.slice(offset, keySize);
            dataBuffer.position(offset + keySize);
            offset += keySize;

            int valueSize = dataBuffer.getInt();
            offset += Integer.BYTES;
            if (curKey.compareTo(key) == 0) {
                ByteBuffer curValue = dataBuffer.slice(offset, valueSize);
                return new BaseEntry<>(curKey, curValue);
            }
            dataBuffer.position(offset + valueSize);
            offset += valueSize;
        }
        return null;*/
    }

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
    }

    private int readPrevIndex() throws IOException {
        Path pathToReadMetaInfo = config.basePath().resolve(META_INFO_FILE_NAME + EXTENSION);
        if (!Files.exists(pathToReadMetaInfo)) {
            return 0;
        }
        try (FileChannel infoChannel = FileChannel.open(pathToReadMetaInfo,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ)) {

            ByteBuffer infoBuffer = infoChannel.map(FileChannel.MapMode.READ_ONLY, 0, Integer.BYTES);
            infoChannel.close();
            return infoBuffer.getInt();
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
                     StandardOpenOption.TRUNCATE_EXISTING);) {

            ByteBuffer writeDataBuffer = mapChannelData(dataChannel);
            ByteBuffer writeIndexBuffer = mapChannelIndex(indexChannel);

            for (Entry<ByteBuffer> el : inMemoryData.values()) {
                writeIndexBuffer.putInt(writeDataBuffer.position());

                writeDataBuffer.putInt(el.key().remaining());
                writeDataBuffer.put(el.key());
                writeDataBuffer.putInt(el.value().remaining());
                writeDataBuffer.put(el.value());
            }
        }

        try (RandomAccessFile file = new RandomAccessFile(config.basePath().resolve(META_INFO_FILE_NAME + EXTENSION).toFile(), "rw")) {
            file.writeInt(index);
        }
    }

    private void writeCurrentIndex(int index) throws IOException {
        Path pathToReadMetaInfo = config.basePath().resolve(META_INFO_FILE_NAME + EXTENSION);
        try (FileChannel infoChannel = FileChannel.open(pathToReadMetaInfo,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer writeInfoBuffer = infoChannel.map(FileChannel.MapMode.READ_WRITE, 0, Integer.BYTES);
            writeInfoBuffer.putInt(index);
        }
    }

    private ByteBuffer mapChannelData(FileChannel channel) throws IOException {
        int size = 0;
        for (Entry<ByteBuffer> el : inMemoryData.values()) {
            size += el.key().remaining() + el.value().remaining();
        }
        size += 2 * inMemoryData.size() * Integer.BYTES;

        return channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
    }

    private ByteBuffer mapChannelIndex(FileChannel channel) throws IOException {
        return channel.map(FileChannel.MapMode.READ_WRITE, 0, (long) inMemoryData.size() * Integer.BYTES);
    }
}
