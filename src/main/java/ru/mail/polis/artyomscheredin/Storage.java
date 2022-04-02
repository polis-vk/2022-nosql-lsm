package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class Storage {
    private static final String DATA_FILE_NAME = "data";
    private static final String INDEXES_FILE_NAME = "indexes";
    private static final String TEMP_FILE_SUFFIX = "indexes";
    private static final String EXTENSION = ".txt";
    private static final int HEADER_SIZE = 1;

    private final Path basePath;
    private final List<Utils.Pair<ByteBuffer>> mappedDiskData;

    public Storage(Path storagePath) throws IOException {
        this.basePath = storagePath;
        this.mappedDiskData = new ArrayList<>();
        while (true) {
            try {
                mapNextStorageUnit();
            } catch (NoSuchFileException e) {
                break;
            }
        }
    }

    public void mapNextStorageUnit() throws IOException, NoSuchFileException {
        Path nextDataFilePath = basePath.resolve(DATA_FILE_NAME + (mappedDiskData.size() + 1) + EXTENSION);
        Path nextIndexFilePath =  basePath.resolve(INDEXES_FILE_NAME + (mappedDiskData.size() + 1) + EXTENSION);
        Utils.Pair<ByteBuffer> mappedUnit = mapOnDiskStorageUnit(nextDataFilePath, nextIndexFilePath);
        mappedDiskData.add(mappedUnit);
    }

    private Utils.Pair<ByteBuffer> mapOnDiskStorageUnit(Path dataPath,
                                                        Path indexPath) throws IOException, NoSuchFileException {
        try (FileChannel dataChannel = FileChannel.open(dataPath);
             FileChannel indexChannel = FileChannel.open(indexPath)) {
            ByteBuffer indexBuffer = indexChannel.map(FileChannel.MapMode.READ_ONLY,
                    0, indexChannel.size());
            ByteBuffer dataBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY,
                    0, dataChannel.size());
            return new Utils.Pair<>(dataBuffer, indexBuffer);
        }
    }

    private static void store(Iterator<BaseEntry<ByteBuffer>> entryIterator,
                              Utils.Pair<Path> pathsToWrite,
                              Utils.Pair<Integer> bufferSizes) throws IOException {
        if (!entryIterator.hasNext()) {
            return;
        }
        ByteBuffer writeDataBuffer;
        ByteBuffer writeIndexBuffer;
        try (FileChannel dataChannel = FileChannel.open(pathsToWrite.first(),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
             FileChannel indexChannel = FileChannel.open(pathsToWrite.second(),
                     StandardOpenOption.CREATE,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.TRUNCATE_EXISTING)) {
            writeDataBuffer = dataChannel.map(FileChannel.MapMode.READ_WRITE, 0, bufferSizes.first());
            writeIndexBuffer = indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, bufferSizes.second());

            //index file: entities_number=n, offset_1...offset_n
            //data file: key_size, key, value_size, value
            writeIndexBuffer.putInt(bufferSizes.second() / Integer.BYTES - 1);
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
    }

    public void save(Iterator<BaseEntry<ByteBuffer>> iterator,
                     Utils.Pair<Integer> dataAndIndexBufferSize,
                     boolean isTemp) throws IOException {
        Path dataPath;
        Path indexPath;
        if (isTemp) {
            dataPath = basePath.resolve(DATA_FILE_NAME + TEMP_FILE_SUFFIX + EXTENSION);
            indexPath = basePath.resolve(INDEXES_FILE_NAME + TEMP_FILE_SUFFIX + EXTENSION);
        } else {
            int index = mappedDiskData.size() + 1;
            dataPath = basePath.resolve(DATA_FILE_NAME + index + EXTENSION);
            indexPath = basePath.resolve(INDEXES_FILE_NAME + index + EXTENSION);
        }
        store(iterator, new Utils.Pair<>(dataPath, indexPath), dataAndIndexBufferSize);
    }

    public static Utils.Pair<Integer> getDataAndIndexBufferSize(Iterator<BaseEntry<ByteBuffer>> it) {
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

    public List<PeekIterator> getListOfOnDiskIterators(ByteBuffer from, ByteBuffer to) {
        List<PeekIterator> iterators = new ArrayList<>();
        int priority = 0;
        for (Utils.Pair<ByteBuffer> pair : mappedDiskData) {
            iterators.add(new PeekIterator(new FileIterator(pair.first(),
                    pair.second(), from, to), priority++));
        }
        return iterators;
    }

    public void cleanDiskExceptTempFile() throws IOException {
        for (int i = 1; ; i++) {
            Path curIndexFilePath = basePath.resolve(INDEXES_FILE_NAME + i + EXTENSION);
            Path curDataFilePath = basePath.resolve(DATA_FILE_NAME + i + EXTENSION);
            if (!Files.deleteIfExists(curIndexFilePath) || !Files.deleteIfExists(curDataFilePath)) {
                break;
            }
        }
        mappedDiskData.clear();
    }

    public void renameTempFile() throws IOException {
        try {
            Path tempDataPath = basePath.resolve(DATA_FILE_NAME + TEMP_FILE_SUFFIX + EXTENSION);
            Path tempIndexPath = basePath.resolve(INDEXES_FILE_NAME + TEMP_FILE_SUFFIX + EXTENSION);
            Files.move(tempDataPath, basePath.resolve(DATA_FILE_NAME + 1 + EXTENSION));
            Files.move(tempIndexPath, basePath.resolve(INDEXES_FILE_NAME + 1 + EXTENSION));
            mapNextStorageUnit();
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
