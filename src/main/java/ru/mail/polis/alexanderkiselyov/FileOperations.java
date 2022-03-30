package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileOperations {
    private long filesCount;
    private static final int BUFFER_SIZE = 4000 * Character.BYTES;
    private static final String FILE_NAME = "myData";
    private static final String FILE_EXTENSION = ".txt";
    private static final String FILE_INDEX_NAME = "myIndex";
    private static final String FILE_INDEX_EXTENSION = ".txt";
    private final Path basePath;
    private final List<Path> ssTables;
    private final List<Path> ssIndexes;
    private final Map<Path, Long> tablesSizes;
    private final List<FileChannel> cTables = new ArrayList<>();
    private final List<FileChannel> cIndexes = new ArrayList<>();

    public FileOperations(Config config) throws IOException {
        basePath = config.basePath();
        tablesSizes = new ConcurrentHashMap<>();
        if (Files.exists(basePath)) {
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
        } else {
            ssTables = new ArrayList<>();
            ssIndexes = new ArrayList<>();
        }
        filesCount = ssTables.size();
        for (int i = 0; i < filesCount; i++) {
            tablesSizes.put(ssIndexes.get(i), indexSize(ssIndexes.get(i)));
        }
    }

    public Iterator<BaseEntry<byte[]>> diskIterator(byte[] from, byte[] to) throws IOException {
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
        FileIterator fi = new FileIterator(ssTable, ssIndex, from, to, indexSize);
        cTables.add(fi.getCTable());
        cIndexes.add(fi.getCIndex());
        return fi;
    }

    static long getEntryIndex(FileChannel cTable, FileChannel cIndex, byte[] key, long indexSize) throws IOException {
        long low = 0;
        long high = indexSize - 1;
        long mid = (low + high) / 2;
        while (low <= high) {
            BaseEntry<byte[]> current = getCurrent(mid, cTable, cIndex);
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

    public void save(NavigableMap<byte[], BaseEntry<byte[]>> pairs) throws IOException {
        for (FileChannel cTable : cTables) {
            if (cTable != null) {
                cTable.close();
            }
        }
        for (FileChannel cIndex : cIndexes) {
            if (cIndex != null) {
                cIndex.close();
            }
        }
        saveData(pairs);
        saveIndexes(pairs);
        filesCount++;
    }

    private void saveData(NavigableMap<byte[], BaseEntry<byte[]>> sortedPairs) throws IOException {
        Path newFilePath = basePath.resolve(FILE_NAME + filesCount + FILE_EXTENSION);
        if (!Files.exists(newFilePath)) {
            Files.createFile(newFilePath);
        }
        ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
        try (FileOutputStream fos = new FileOutputStream(String.valueOf(newFilePath));
             BufferedOutputStream writer = new BufferedOutputStream(fos, BUFFER_SIZE)) {
            for (var pair : sortedPairs.entrySet()) {
                intBuffer.putInt(pair.getKey().length);
                writer.write(intBuffer.array());
                intBuffer.clear();
                writer.write(pair.getKey());
                if (pair.getValue().value() == null) {
                    intBuffer.putInt(-1);
                    writer.write(intBuffer.array());
                    intBuffer.clear();
                } else {
                    intBuffer.putInt(pair.getValue().value().length);
                    writer.write(intBuffer.array());
                    intBuffer.clear();
                    writer.write(pair.getValue().value());
                }
            }
        }
    }

    private void saveIndexes(NavigableMap<byte[], BaseEntry<byte[]>> sortedPairs) throws IOException {
        Path newIndexPath = basePath.resolve(FILE_INDEX_NAME + filesCount + FILE_INDEX_EXTENSION);
        if (!Files.exists(newIndexPath)) {
            Files.createFile(newIndexPath);
        }
        long size = 0;
        ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
        try (FileOutputStream fos = new FileOutputStream(String.valueOf(newIndexPath));
             BufferedOutputStream writer = new BufferedOutputStream(fos, BUFFER_SIZE)) {
            longBuffer.putLong(sortedPairs.size());
            writer.write(longBuffer.array());
            longBuffer.clear();
            longBuffer.putLong(0);
            writer.write(longBuffer.array());
            longBuffer.clear();
            for (var pair : sortedPairs.entrySet()) {
                if (pair.getValue().value() == null) {
                    size += 2 * Integer.BYTES + pair.getKey().length;
                } else {
                    size += 2 * Integer.BYTES + pair.getKey().length + pair.getValue().value().length;
                }
                longBuffer.putLong(size);
                writer.write(longBuffer.array());
                longBuffer.clear();
            }
        }
    }

    private long indexSize(Path indexPath) throws IOException {
        long size;
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        try (FileInputStream fis = new FileInputStream(String.valueOf(indexPath));
             BufferedInputStream reader = new BufferedInputStream(fis, BUFFER_SIZE)) {
            buffer.put(reader.readNBytes(Long.BYTES));
            buffer.flip();
            size = buffer.getLong();
        }
        return size;
    }

    static BaseEntry<byte[]> getCurrent(long pos, FileChannel cTable, FileChannel cIndex) throws IOException {
        long position;
        cIndex.position((pos + 1) * Long.BYTES);
        ByteBuffer buffLong = ByteBuffer.allocate(Long.BYTES);
        cIndex.read(buffLong);
        buffLong.flip();
        position = buffLong.getLong();

        cTable.position(position);
        ByteBuffer buffInt = ByteBuffer.allocate(Integer.BYTES);
        cTable.read(buffInt);
        buffInt.flip();
        int keyLength = buffInt.getInt();
        buffInt.clear();
        ByteBuffer currentKey = ByteBuffer.allocate(keyLength);
        cTable.read(currentKey);

        cTable.read(buffInt);
        buffInt.flip();
        int valueLength = buffInt.getInt();
        if (valueLength == -1) {
            return new BaseEntry<>(currentKey.array(), null);
        }
        ByteBuffer currentValue = ByteBuffer.allocate(valueLength);
        cTable.read(currentValue);
        return new BaseEntry<>(currentKey.array(), currentValue.array());
    }
}
