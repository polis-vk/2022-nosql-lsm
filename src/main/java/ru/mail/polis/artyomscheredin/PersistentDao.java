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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> inMemoryData =
            new ConcurrentSkipListMap<>(ByteBuffer::compareTo);

    private final Storage storage;

    public PersistentDao(Config config) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException();
        }
        this.storage = new Storage(config);

    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        lock.readLock().lock();
        try {
            List<PeekIterator> iteratorsList = new ArrayList<>();
            int priority = 0;
            iteratorsList.addAll(storage.getIterators);
            for (Utils.MappedBuffersPair pair : mappedDiskData) {
                iteratorsList.add(new PeekIterator(new FileIterator(pair.getDataBuffer(),
                        pair.getIndexBuffer(), from, to), priority++));
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
            store(inMemoryData);
            inMemoryData.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private int readPrevIndex() throws IOException {
        Path pathToReadMetaInfo = config.basePath().resolve(META_INFO_FILE_NAME + EXTENSION);
        try {
            ByteBuffer temp = ByteBuffer.wrap(Files.readAllBytes(pathToReadMetaInfo));
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
        int index = mappedDiskData.size() + 1;

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
                if (el.value() == null) {
                    size += el.key().remaining();
                } else {
                    size += el.key().remaining() + el.value().remaining();
                }
            }
            size += 2 * inMemoryData.size() * Integer.BYTES;

            ByteBuffer writeDataBuffer = dataChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
            ByteBuffer writeIndexBuffer = indexChannel.map(FileChannel.MapMode.READ_WRITE, 0,
                    (long) (HEADER_SIZE + inMemoryData.size()) * Integer.BYTES);


            //index file: entities_number=n, offset_1...offset_n
            //data file: key_size, key, value_size, value
            writeIndexBuffer.putInt(inMemoryData.size());
            for (Entry<ByteBuffer> el : inMemoryData.values()) {
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

        try (RandomAccessFile file = new RandomAccessFile(config.basePath()
                .resolve(META_INFO_FILE_NAME + EXTENSION).toFile(), "rw")) {
            file.writeInt(index);
        }
    }

    @Override
    public void compact() {
        writeTemp();
        deleteOld();
        renameTemp();
    }


    private static class FileIterator implements Iterator<BaseEntry<ByteBuffer>> {
        private final ByteBuffer dataBuffer;
        private final ByteBuffer indexBuffer;
        private final int upperBound;
        private int cursor;

        public FileIterator(ByteBuffer dataBuffer, ByteBuffer indexBuffer, ByteBuffer from, ByteBuffer to) {
            this.dataBuffer = dataBuffer;
            this.indexBuffer = indexBuffer;
            cursor = (from == null) ? HEADER_SIZE : findOffset(indexBuffer, dataBuffer, from);
            upperBound = (to == null) ? indexBuffer.limit() : findOffset(indexBuffer, dataBuffer, to);
        }

        private static int findOffset(ByteBuffer indexBuffer, ByteBuffer dataBuffer, ByteBuffer key) {
            int low = HEADER_SIZE;
            int mid = 0;
            int high = indexBuffer.remaining()/ Integer.BYTES - 1;
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
