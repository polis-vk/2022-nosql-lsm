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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class FileOperations {
    private final AtomicInteger filesCount;
    private final Path basePath;
    private final List<Path> ssTables;
    private final List<Path> ssIndexes;
    private final Map<Path, Long> tablesSizes;
    private final List<FileIterator> fileIterators = new ArrayList<>();
    private final CompactOperations compactOperations;
    private static final String FILE_NAME = "myData";
    private static final String FILE_EXTENSION = ".dat";
    private static final String FILE_INDEX_NAME = "myIndex";
    private static final String FILE_INDEX_EXTENSION = ".idx";

    public FileOperations(Config config) throws IOException {
        basePath = config.basePath();
        ssTables = new ArrayList<>();
        ssIndexes = new ArrayList<>();
        tablesSizes = new ConcurrentHashMap<>();
        filesCount = new AtomicInteger();
        compactOperations = new CompactOperations(FILE_NAME, FILE_EXTENSION, FILE_INDEX_NAME, FILE_INDEX_EXTENSION);
        Map<Path, Path> allData = compactOperations.checkFiles(basePath);
        getDataInfo(allData);
    }

    private void getDataInfo(Map<Path, Path> allData) throws IOException {
        filesCount.set(allData.size());
        for (Map.Entry<Path, Path> entry : allData.entrySet()) {
            ssTables.add(entry.getKey());
            ssIndexes.add(entry.getValue());
        }
        for (int i = 0; i < filesCount.get(); i++) {
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
        if (tablesSizes == null || tablesSizes.get(ssIndex) == null) {
            return null;
        }
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
        if (filesCount.get() <= 1 && !hasPairs) {
            return;
        }
        compactOperations.saveDataAndIndexesCompact(iterator, basePath);
        compactOperations.clearFileIterators(fileIterators);
        compactOperations.deleteAllFiles(ssTables, ssIndexes);
        compactOperations.renameCompactedFile(basePath);
        ssTables.clear();
        ssIndexes.clear();
        tablesSizes.clear();
        Path indexPath = basePath.resolve(FILE_INDEX_NAME + "0" + FILE_INDEX_EXTENSION);
        ssTables.add(basePath.resolve(FILE_NAME + "0" + FILE_EXTENSION));
        ssIndexes.add(indexPath);
        tablesSizes.put(indexPath, indexSize(indexPath));
        filesCount.set(1);
    }

    void flush(NavigableMap<byte[], BaseEntry<byte[]>> pairs) throws IOException {
        if (pairs == null) {
            return;
        }
        saveDataAndIndexes(pairs);
    }

    private void saveDataAndIndexes(NavigableMap<byte[], BaseEntry<byte[]>> sortedPairs) throws IOException {
        Path newFilePath = basePath.resolve(FILE_NAME + filesCount + FILE_EXTENSION);
        Path newIndexPath = basePath.resolve(FILE_INDEX_NAME + filesCount + FILE_INDEX_EXTENSION);
        filesCount.incrementAndGet();
        Files.createFile(newFilePath);
        Files.createFile(newIndexPath);
        ssTables.add(newFilePath);
        ssIndexes.add(newIndexPath);
        long offset = 0;
        try (FileReaderWriter writer = new FileReaderWriter(newFilePath, newIndexPath)) {
            writeFileSizeAndInitialPosition(writer.getIndexChannel(), sortedPairs.size());
            for (var pair : sortedPairs.entrySet()) {
                writePair(writer.getFileChannel(), pair);
                offset = writeEntryPosition(writer.getIndexChannel(), pair, offset);
            }
        }
        tablesSizes.put(newIndexPath, indexSize(newIndexPath));
    }

    static void writePair(FileChannel channel, Map.Entry<byte[], BaseEntry<byte[]>> pair) throws IOException {
        ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
        intBuffer.putInt(pair.getKey().length);
        intBuffer.flip();
        channel.write(intBuffer);
        intBuffer.clear();
        channel.write(ByteBuffer.wrap(pair.getKey()));
        if (pair.getValue().value() == null) {
            intBuffer.putInt(-1);
            intBuffer.flip();
            channel.write(intBuffer);
            intBuffer.clear();
        } else {
            intBuffer.putInt(pair.getValue().value().length);
            intBuffer.flip();
            channel.write(intBuffer);
            intBuffer.clear();
            channel.write(ByteBuffer.wrap(pair.getValue().value()));
        }
    }

    private static void writeFileSizeAndInitialPosition(FileChannel channel, long pairsSize) throws IOException {
        ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
        longBuffer.putLong(pairsSize);
        longBuffer.flip();
        channel.write(longBuffer);
        longBuffer.clear();
        longBuffer.putLong(0);
        longBuffer.flip();
        channel.write(longBuffer);
        longBuffer.clear();
    }

    static long writeEntryPosition(FileChannel channel, Map.Entry<byte[],
            BaseEntry<byte[]>> pair, long size) throws IOException {
        ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
        long result = size;
        if (pair.getValue().value() == null) {
            result += 2 * Integer.BYTES + pair.getKey().length;
        } else {
            result += 2 * Integer.BYTES + pair.getKey().length + pair.getValue().value().length;
        }
        longBuffer.putLong(result);
        longBuffer.flip();
        channel.write(longBuffer);
        longBuffer.clear();
        return result;
    }

    private static long indexSize(Path indexPath) throws IOException {
        long size;
        try (RandomAccessFile raf = new RandomAccessFile(indexPath.toString(), "r")) {
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
