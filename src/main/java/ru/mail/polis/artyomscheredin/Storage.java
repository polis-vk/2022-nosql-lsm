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

public class Storage {
    private static final String DATA_FILE_NAME = "data";
    private static final String INDEXES_FILE_NAME = "indexes";
    private static final String TEMP_FILE_SUFFIX = "indexes";
    private static final String EXTENSION = ".txt";
    /**
     * Header size in the beginning of index file in bytes.
     */
    private static final int INDEX_HEADER_SIZE = 1;

    private final Path basePath;
    private final List<Utils.Pair<ByteBuffer>> mappedDiskData;

    @SuppressWarnings("StatementWithEmptyBody")
    public Storage(Path storagePath) throws IOException {
        this.basePath = storagePath;
        this.mappedDiskData = new ArrayList<>();

        ByteBuffer tempBuffer = mapTempFileIfExists();
        if ((tempBuffer != null) && !isDamaged(tempBuffer)) {
            cleanDiskExceptTempFile(basePath);
            renameTempFile(basePath);
        }
        while (mapNextStorageUnit()) {
            //Mapping next table
        }
        mappedDiskData.forEach(e -> {
            if (isDamaged(e.index())) {
                throw new RuntimeException("Dao disk storage is damaged");
            }
        });
    }

    private ByteBuffer mapTempFileIfExists() throws IOException {
        try (FileChannel indexChannel
                     = FileChannel.open(basePath.resolve(INDEXES_FILE_NAME + TEMP_FILE_SUFFIX + EXTENSION))) {
            return indexChannel.map(FileChannel.MapMode.READ_ONLY,
                    0, indexChannel.size());
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    private boolean isDamaged(ByteBuffer indexBuffer) {
        int size = indexBuffer.getInt(0);
        return size != ((indexBuffer.remaining() / Integer.BYTES) - INDEX_HEADER_SIZE);
    }

    public final boolean mapNextStorageUnit() throws IOException {
        Path nextDataFilePath = basePath.resolve(DATA_FILE_NAME + (mappedDiskData.size() + 1) + EXTENSION);
        Path nextIndexFilePath = basePath.resolve(INDEXES_FILE_NAME + (mappedDiskData.size() + 1) + EXTENSION);
        Utils.Pair<ByteBuffer> mappedUnit;
        try {
            mappedUnit = mapOnDiskStorageUnit(nextDataFilePath, nextIndexFilePath);
        } catch (NoSuchFileException e) {
            return false;
        }
        mappedDiskData.add(mappedUnit);
        return true;
    }

    private static Utils.Pair<ByteBuffer> mapOnDiskStorageUnit(Path dataPath,
                                                        Path indexPath) throws IOException {
        try (FileChannel dataChannel = FileChannel.open(dataPath);
             FileChannel indexChannel = FileChannel.open(indexPath)) {
            ByteBuffer indexBuffer = indexChannel.map(FileChannel.MapMode.READ_ONLY,
                    0, indexChannel.size());
            ByteBuffer dataBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY,
                    0, dataChannel.size());
            return new Utils.Pair<>(dataBuffer, indexBuffer);
        }
    }

    public void store(Iterator<BaseEntry<ByteBuffer>> entryIterator,
                      Utils.Pair<Integer> bufferSizes,
                      boolean isTemp) throws IOException {
        if (!entryIterator.hasNext()) {
            return;
        }

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

        ByteBuffer writeDataBuffer;
        ByteBuffer writeIndexBuffer;
        try (FileChannel dataChannel = FileChannel.open(dataPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
             FileChannel indexChannel = FileChannel.open(indexPath,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.TRUNCATE_EXISTING)) {
            writeDataBuffer = dataChannel.map(FileChannel.MapMode.READ_WRITE, 0, bufferSizes.data());
            writeIndexBuffer = indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, bufferSizes.index());

            //index file: entities_number=n, offset_1...offset_n
            //data file: key_size, key, value_size, value
            writeIndexBuffer.putInt(bufferSizes.index() / Integer.BYTES - 1);
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
        return new Utils.Pair<>(size, (INDEX_HEADER_SIZE + count) * Integer.BYTES);
    }

    public List<PeekIterator> getListOfOnDiskIterators(ByteBuffer from, ByteBuffer to) {
        List<PeekIterator> iterators = new ArrayList<>();
        int priority = 0;
        for (Utils.Pair<ByteBuffer> pair : mappedDiskData) {
            iterators.add(new PeekIterator(new FileIterator(pair.data(),
                    pair.index(), from, to), priority++));
        }
        return iterators;
    }

    public static void cleanDiskExceptTempFile(Path basePath) throws IOException {
        for (int i = 1; ; i++) {
            Path curIndexFilePath = basePath.resolve(INDEXES_FILE_NAME + i + EXTENSION);
            Path curDataFilePath = basePath.resolve(DATA_FILE_NAME + i + EXTENSION);
            if (!Files.deleteIfExists(curIndexFilePath) || !Files.deleteIfExists(curDataFilePath)) {
                break;
            }
        }
    }

    @SuppressWarnings("EmptyCatchBlock")
    public static boolean renameTempFile(Path basePath) throws IOException {
        try {
            Path tempDataPath = basePath.resolve(DATA_FILE_NAME + TEMP_FILE_SUFFIX + EXTENSION);
            Path tempIndexPath = basePath.resolve(INDEXES_FILE_NAME + TEMP_FILE_SUFFIX + EXTENSION);
            Files.move(tempDataPath, basePath.resolve(DATA_FILE_NAME + 1 + EXTENSION));
            Files.move(tempIndexPath, basePath.resolve(INDEXES_FILE_NAME + 1 + EXTENSION));
        } catch (NoSuchFileException e) {
            //Dao was empty, temp file hasn't created
            return false;
        }
        return true;
    }

    public static int getIndexHeaderSize() {
        return INDEX_HEADER_SIZE;
    }

    public Path getBasePath() {
        return basePath;
    }

    public void cleanMappedData() {
        mappedDiskData.clear();
    }
}
