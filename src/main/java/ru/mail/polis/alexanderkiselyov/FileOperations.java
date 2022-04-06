package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileOperations {
    private long filesCount;
    private static final String FILE_NAME = "myData";
    private static final String FILE_EXTENSION = ".dat";
    private static final String FILE_INDEX_NAME = "myIndex";
    private static final String FILE_INDEX_EXTENSION = ".idx";
    private static final String FILE_TMP_NAME = "tmp";
    private static final String FILE_TMP_EXTENSION = ".dat";
    private static final String FILE_INDEX_TMP_NAME = "tmpIndex";
    private static final String FILE_INDEX_TMP_EXTENSION = ".idx";
    private final Path basePath;
    private List<Path> ssTables;
    private List<Path> ssIndexes;
    private final Map<Path, Long> tablesSizes;
    private final List<FileIterator> fileIterators = new ArrayList<>();
    private final CompactOperations compactOperations;

    public FileOperations(Config config) throws IOException {
        basePath = config.basePath();
        tablesSizes = new ConcurrentHashMap<>();
        Map<String, String> fileNames = new HashMap<>();
        fileNames.put("fileName", FILE_NAME);
        fileNames.put("fileExtension", FILE_EXTENSION);
        fileNames.put("fileIndexName", FILE_INDEX_NAME);
        fileNames.put("fileIndexExtension", FILE_INDEX_EXTENSION);
        fileNames.put("fileTmpName", FILE_TMP_NAME);
        fileNames.put("fileTmpExtension", FILE_TMP_EXTENSION);
        fileNames.put("fileIndexTmpName", FILE_INDEX_TMP_NAME);
        fileNames.put("fileIndexTmpExtension", FILE_INDEX_TMP_EXTENSION);
        getDataInfo();
        compactOperations = new CompactOperations(fileNames);
        if (Files.exists(basePath.resolve(FILE_TMP_NAME + FILE_TMP_EXTENSION))) {
            compact(new SkipNullValuesIterator(new IndexedPeekIterator(0, diskIterator(null, null))), false);
            getDataInfo();
        }
    }

    private void getDataInfo() throws IOException {
        try (Stream<Path> pathStream = Files.list(basePath)) {
            ssTables = pathStream.toList().stream()
                    .filter(f -> String.valueOf(f.getFileName()).contains(FILE_NAME))
                    .sorted(new PathsComparator(FILE_NAME, FILE_EXTENSION))
                    .collect(Collectors.toList());
        }
        try (Stream<Path> indexPathStream = Files.list(basePath)) {
            ssIndexes = indexPathStream.toList().stream()
                    .filter(f -> String.valueOf(f.getFileName()).contains(FILE_INDEX_NAME))
                    .sorted(new PathsComparator(FILE_INDEX_NAME, FILE_INDEX_EXTENSION))
                    .collect(Collectors.toList());
        }
        filesCount = ssTables.size();
        for (int i = 0; i < filesCount; i++) {
            tablesSizes.put(ssIndexes.get(i), indexSize(ssIndexes.get(i)));
        }
    }

    Iterator<BaseEntry<byte[]>> diskIterator(byte[] from, byte[] to) throws IOException {
        List<IndexedPeekIterator> peekIterators = new ArrayList<>();
        for (int i = 0; i < ssTables.size(); i++) {
            Iterator<BaseEntry<byte[]>> iterator = diskIterator(ssTables.get(i), ssIndexes.get(i), from, to);
            peekIterators.add(new IndexedPeekIterator(i, iterator));
        }
        return MergeIterator.of(peekIterators, EntryKeyComparator.INSTANCE);
    }

    private Iterator<BaseEntry<byte[]>> diskIterator(Path ssTable, Path ssIndex, byte[] from, byte[] to)
            throws IOException {
        long indexSize = tablesSizes.get(ssIndex);
        FileIterator fileIterator = new FileIterator(ssTable, ssIndex, from, to, indexSize);
        fileIterators.add(fileIterator);
        return fileIterator;
    }

    static long getEntryIndex(FileChannel channelTable, FileChannel channelIndex,
                              byte[] key, long indexSize) throws IOException {
        long low = 0;
        long high = indexSize - 1;
        long mid = (low + high) / 2;
        while (low <= high) {
            BaseEntry<byte[]> current = getCurrent(mid, channelTable, channelIndex);
            int compare = Arrays.compare(key, current.key());
            if (compare > 0) {
                low = mid + 1;
            } else if (compare < 0) {
                high = mid - 1;
            } else {
                return mid;
            }
            mid = (low + high) / 2;
        }
        return low;
    }

    void compact(Iterator<BaseEntry<byte[]>> iterator, boolean hasPairs) throws IOException {
        if (filesCount <= 1 && !hasPairs) {
            return;
        }
        compactOperations.createCompactedFiles(basePath);
        compactOperations.saveDataAndIndexesCompact(iterator);
        compactOperations.clearFileIterators(fileIterators);
        compactOperations.deleteAllFiles(ssTables, ssIndexes, filesCount);
        compactOperations.renameCompactedFile(basePath);
        ssTables.clear();
        ssIndexes.clear();
        tablesSizes.clear();
        filesCount = 1;
    }

    void flush(NavigableMap<byte[], BaseEntry<byte[]>> pairs) throws IOException {
        saveData(pairs);
        saveIndexes(pairs);
        filesCount++;
    }

    private void saveData(NavigableMap<byte[], BaseEntry<byte[]>> sortedPairs) throws IOException {
        if (sortedPairs == null) {
            return;
        }
        Path newFilePath = basePath.resolve(FILE_NAME + filesCount + FILE_EXTENSION);
        if (!Files.exists(newFilePath)) {
            Files.createFile(newFilePath);
        }
        try (RandomAccessFile raf = new RandomAccessFile(String.valueOf(newFilePath), "rw")) {
            for (var pair : sortedPairs.entrySet()) {
                writePair(raf, pair);
            }
        }
    }

    private void saveIndexes(NavigableMap<byte[], BaseEntry<byte[]>> sortedPairs) throws IOException {
        if (sortedPairs == null) {
            return;
        }
        Path newIndexPath = basePath.resolve(FILE_INDEX_NAME + filesCount + FILE_INDEX_EXTENSION);
        if (!Files.exists(newIndexPath)) {
            Files.createFile(newIndexPath);
        }
        long offset = 0;
        try (RandomAccessFile raf = new RandomAccessFile(String.valueOf(newIndexPath), "rw")) {
            writeFileSizeAndInitialPosition(raf, sortedPairs.size());
            for (var pair : sortedPairs.entrySet()) {
                offset = writeEntryPosition(raf, pair, offset);
            }
        }
    }

    static void writePair(RandomAccessFile raf, Map.Entry<byte[], BaseEntry<byte[]>> pair) throws IOException {
        ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
        intBuffer.putInt(pair.getKey().length);
        raf.write(intBuffer.array());
        intBuffer.clear();
        raf.write(pair.getKey());
        if (pair.getValue().value() == null) {
            intBuffer.putInt(-1);
            raf.write(intBuffer.array());
            intBuffer.clear();
        } else {
            intBuffer.putInt(pair.getValue().value().length);
            raf.write(intBuffer.array());
            intBuffer.clear();
            raf.write(pair.getValue().value());
        }
    }

    private void writeFileSizeAndInitialPosition(RandomAccessFile raf, long pairsSize) throws IOException {
        ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
        longBuffer.putLong(pairsSize);
        raf.write(longBuffer.array());
        longBuffer.clear();
        longBuffer.putLong(0);
        raf.write(longBuffer.array());
        longBuffer.clear();
    }

    static long writeEntryPosition(RandomAccessFile raf, Map.Entry<byte[],
            BaseEntry<byte[]>> pair, long size) throws IOException {
        ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
        long result = size;
        if (pair.getValue().value() == null) {
            result += 2 * Integer.BYTES + pair.getKey().length;
        } else {
            result += 2 * Integer.BYTES + pair.getKey().length + pair.getValue().value().length;
        }
        longBuffer.putLong(result);
        raf.write(longBuffer.array());
        longBuffer.clear();
        return result;
    }

    private long indexSize(Path indexPath) throws IOException {
        long size;
        try (RandomAccessFile raf = new RandomAccessFile(String.valueOf(indexPath), "r")) {
            size = raf.readLong();
        }
        return size;
    }

    static BaseEntry<byte[]> getCurrent(long pos, FileChannel channelTable,
                                        FileChannel channelIndex) throws IOException {
        long position;
        channelIndex.position((pos + 1) * Long.BYTES);
        ByteBuffer buffLong = ByteBuffer.allocate(Long.BYTES);
        channelIndex.read(buffLong);
        buffLong.flip();
        position = buffLong.getLong();
        channelTable.position(position);
        ByteBuffer buffInt = ByteBuffer.allocate(Integer.BYTES);
        channelTable.read(buffInt);
        buffInt.flip();
        int keyLength = buffInt.getInt();
        buffInt.clear();
        ByteBuffer currentKey = ByteBuffer.allocate(keyLength);
        channelTable.read(currentKey);
        channelTable.read(buffInt);
        buffInt.flip();
        int valueLength = buffInt.getInt();
        if (valueLength == -1) {
            return new BaseEntry<>(currentKey.array(), null);
        }
        ByteBuffer currentValue = ByteBuffer.allocate(valueLength);
        channelTable.read(currentValue);
        return new BaseEntry<>(currentKey.array(), currentValue.array());
    }

    public void clearFileIterators() throws IOException {
        compactOperations.clearFileIterators(fileIterators);
    }
}
