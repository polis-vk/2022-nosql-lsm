package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private static final String DATA_FILE_NAME = "data";
    private static final String INDEXES_FILE_NAME = "indexes";
    private static final String TEMP_FILE_SUFFIX = "indexes";
    private static final String EXTENSION = ".txt";
    private static final int HEADER_SIZE = 1;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> inMemoryData =
            new ConcurrentSkipListMap<>(ByteBuffer::compareTo);
    private final List<Utils.Pair<ByteBuffer>> mappedDiskData;
    private final Config config;

    public PersistentDao(Config config) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException();
        }
        this.config = config;


        List<Utils.Pair<ByteBuffer>> list = new LinkedList<>();
        for (int i = 1; ; i++) {
            try (FileChannel dataChannel = FileChannel
                    .open(config.basePath().resolve(DATA_FILE_NAME + i + EXTENSION));
                 FileChannel indexChannel = FileChannel
                         .open(config.basePath().resolve(INDEXES_FILE_NAME + i + EXTENSION))) {
                ByteBuffer indexBuffer = indexChannel
                        .map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
                ByteBuffer dataBuffer = dataChannel
                        .map(FileChannel.MapMode.READ_ONLY, 0, dataChannel.size());
                list.add(new Utils.Pair<>(dataBuffer, indexBuffer));
            } catch (NoSuchFileException e) {
                break;
            }
        }
        this.mappedDiskData = list;
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        lock.readLock().lock();
        try {
            List<PeekIterator> iteratorsList = new ArrayList<>();
            int priority = 0;
            for (Utils.Pair<ByteBuffer> pair : mappedDiskData) {
                iteratorsList.add(new PeekIterator(new FileIterator(pair.first(),
                        pair.second(), from, to), priority++));
            }
            if (!inMemoryData.isEmpty()) {
                iteratorsList.add(new PeekIterator(getInMemoryIterator(from, to), priority));
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
            int index = mappedDiskData.size() + 1;
            Utils.Pair<Integer> dataAndIndexBufferSize = getDataAndIndexBufferSize(inMemoryData.values().iterator());
            store(inMemoryData.values().iterator(), config.basePath().resolve(DATA_FILE_NAME + index + EXTENSION),
                    config.basePath().resolve(INDEXES_FILE_NAME + index + EXTENSION),
                    dataAndIndexBufferSize.first(),
                    dataAndIndexBufferSize.second());

            inMemoryData.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Utils.Pair<Integer> getDataAndIndexBufferSize(Iterator<BaseEntry<ByteBuffer>> it) {
        int size = 0;
        int count = 0;
        while (it.hasNext()) {
            BaseEntry<ByteBuffer> el = it.next();
            count++;
            if (el.value() == null) {
                size += el.key().remaining();
            } else {
                size += el.key().remaining() + el.value().remaining();
            }
        }
        size += 2 * count * Integer.BYTES;
        return new Utils.Pair<>(size, (HEADER_SIZE + count) * Integer.BYTES);
    }

    private Utils.Pair<ByteBuffer> store(Iterator<BaseEntry<ByteBuffer>> entryIterator,
                             Path pathToWriteData,
                             Path pathToWriteIndexes, int dataFileSize, int indexesFileSize) throws IOException {
        if (!entryIterator.hasNext()) {
            return null;
        }
        ByteBuffer writeDataBuffer;
        ByteBuffer writeIndexBuffer;
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
            writeDataBuffer = dataChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataFileSize);
            writeIndexBuffer = indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexesFileSize);

            //index file: entities_number=n, offset_1...offset_n
            //data file: key_size, key, value_size, value
            writeIndexBuffer.putInt(indexesFileSize / Integer.BYTES - 1);
            while (entryIterator.hasNext()) {
                BaseEntry<ByteBuffer> el = entryIterator.next();
                writeIndexBuffer.putInt(writeDataBuffer.position());

                writeDataBuffer.putInt(el.key().remaining());
                writeDataBuffer.put(el.key());
                if (el.value() == null) {
                    writeDataBuffer.putInt(-1);
                } else {
                    writeDataBuffer.putInt(el.value().remaining());
                    writeDataBuffer.put(el.value());
                }
            }
        }
        return new Utils.Pair<>(writeDataBuffer, writeIndexBuffer);
    }

    @Override
    public void compact() throws IOException {
        Path tempDataPath = config.basePath().resolve(DATA_FILE_NAME + TEMP_FILE_SUFFIX + EXTENSION);
        Path tempIndexPath = config.basePath().resolve(INDEXES_FILE_NAME + TEMP_FILE_SUFFIX + EXTENSION);
        Utils.Pair<Integer> dataAndIndexBufferSize = getDataAndIndexBufferSize(get(null, null));
        Utils.Pair<ByteBuffer> temp = store(get(null, null), tempDataPath, tempIndexPath,
                dataAndIndexBufferSize.first(), dataAndIndexBufferSize.second());
        inMemoryData.clear();
        mappedDiskData.clear();

        for (int i = 1; ; i++) {
            Path curIndexFilePath = config.basePath().resolve(INDEXES_FILE_NAME + i + EXTENSION);
            Path curDataFilePath = config.basePath().resolve(DATA_FILE_NAME + i + EXTENSION);
            if (!Files.deleteIfExists(curIndexFilePath) || !Files.deleteIfExists(curDataFilePath)) {
                break;
            }
        }

        try {
            Files.move(tempDataPath, config.basePath().resolve(DATA_FILE_NAME + 1 + EXTENSION));
            Files.move(tempIndexPath, config.basePath().resolve(INDEXES_FILE_NAME + 1 + EXTENSION));
            if (temp != null) {
                mappedDiskData.add(temp);
            }
        } catch (NoSuchFileException e) {
            //Dao was empty, temp file hasn't created
        }
    }

    private static class FileIterator implements Iterator<BaseEntry<ByteBuffer>> {
        private final ByteBuffer dataBuffer;
        private final ByteBuffer indexBuffer;
        private final int upperBound;
        private int cursor;

        public FileIterator(ByteBuffer dataBuffer, ByteBuffer indexBuffer, ByteBuffer from, ByteBuffer to) {
            this.dataBuffer = dataBuffer;
            this.indexBuffer = indexBuffer;
            cursor = (from == null) ? HEADER_SIZE * Integer.BYTES : findOffset(indexBuffer, dataBuffer, from);
            upperBound = (to == null) ? indexBuffer.limit() : findOffset(indexBuffer, dataBuffer, to);
        }

        private static int findOffset(ByteBuffer indexBuffer, ByteBuffer dataBuffer, ByteBuffer key) {
            int low = HEADER_SIZE;
            int mid;
            int high = indexBuffer.remaining() / Integer.BYTES - 1;
            while (low <= high) {
                mid = low + ((high - low) / 2);
                int offset = indexBuffer.getInt(mid * Integer.BYTES);
                int keySize = dataBuffer.getInt(offset);

                ByteBuffer curKey = dataBuffer.slice(offset + Integer.BYTES, keySize);
                if (curKey.compareTo(key) < 0) {
                    low = 1 + mid;
                } else if (curKey.compareTo(key) > 0) {
                    high = mid - 1;
                } else if (curKey.compareTo(key) == 0) {
                    return mid * Integer.BYTES;
                }
            }
            return low * Integer.BYTES;
        }

        @Override
        public boolean hasNext() {
            return cursor < upperBound;
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            BaseEntry<ByteBuffer> result = Utils.readEntry(dataBuffer, indexBuffer.getInt(cursor));
            cursor += Integer.BYTES;
            return result;
        }
    }
}
