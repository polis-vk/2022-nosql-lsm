package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {
    private final NavigableMap<byte[], BaseEntry<byte[]>> pairs;
    private final Config config;
    private static final int bufferSize = 200 * Character.BYTES;
    private static final String FILE_NAME = "myData";
    private static final String FILE_EXTENTION = ".txt";
    private static final String FILE_INDEX_NAME = "myIndex";
    private static final String FILE_INDEX_EXTENTION = ".txt";
    private long filesCount;

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        pairs = new ConcurrentSkipListMap<>(Arrays::compare);
        if (Files.exists(config.basePath())) {
            try (Stream<Path> stream = Files.list(config.basePath())) {
                filesCount = stream.filter(f -> String.valueOf(f.getFileName()).contains(FILE_NAME)).count();
            }
        } else {
            filesCount = 0;
        }
    }

    @Override
    public Iterator<BaseEntry<byte[]>> get(byte[] from, byte[] to) {
        if (from == null && to == null) {
            return pairs.values().iterator();
        } else if (from == null) {
            return pairs.headMap(to).values().iterator();
        } else if (to == null) {
            return pairs.tailMap(from).values().iterator();
        }
        return pairs.subMap(from, to).values().iterator();
    }

    @Override
    public BaseEntry<byte[]> get(byte[] key) throws IOException {
        BaseEntry<byte[]> value = pairs.get(key);
        if (value != null && Arrays.equals(value.key(), key)) {
            return value;
        }
        return FindInFiles(key);
    }

    @Override
    public void upsert(BaseEntry<byte[]> entry) {
        pairs.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        Path newFilePath = config.basePath().resolve(FILE_NAME + filesCount + FILE_EXTENTION);
        Path newIndexPath = config.basePath().resolve(FILE_INDEX_NAME + filesCount + FILE_INDEX_EXTENTION);
        if (!Files.exists(newFilePath)) {
            Files.createFile(newFilePath);
        }
        if (!Files.exists(newIndexPath)) {
            Files.createFile(newIndexPath);
        }
        LinkedHashMap<byte[], BaseEntry<byte[]>> sortedPairs = pairs
                .entrySet()
                .stream()
                .sorted(NavigableMap.Entry.comparingByKey(Arrays::compare))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));
        SaveData(newFilePath, sortedPairs);
        SaveIndexes(newIndexPath, sortedPairs);
        filesCount++;
        sortedPairs.clear();
        pairs.clear();
    }

    private void SaveData(Path path, LinkedHashMap<byte[], BaseEntry<byte[]>> sortedPairs) throws IOException {
        ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
        try (FileOutputStream fos = new FileOutputStream(String.valueOf(path));
             BufferedOutputStream writer = new BufferedOutputStream(fos, bufferSize)) {
            for (var pair : sortedPairs.entrySet()) {
                intBuffer.putInt(pair.getKey().length);
                writer.write(intBuffer.array());
                intBuffer.clear();
                writer.write(pair.getKey());
                intBuffer.putInt(pair.getValue().value().length);
                writer.write(intBuffer.array());
                intBuffer.clear();
                writer.write(pair.getValue().value());
            }
        }
    }

    private void SaveIndexes(Path indexPath, LinkedHashMap<byte[], BaseEntry<byte[]>> sortedPairs) throws IOException {
        long size = 0;
        ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
        try (FileOutputStream fos = new FileOutputStream(String.valueOf(indexPath));
             BufferedOutputStream writer = new BufferedOutputStream(fos, bufferSize)) {
            longBuffer.putLong(sortedPairs.size());
            writer.write(longBuffer.array());
            longBuffer.clear();
            longBuffer.putLong(0);
            writer.write(longBuffer.array());
            longBuffer.clear();
            for (var pair : sortedPairs.entrySet()) {
                size += 2 * Integer.BYTES + pair.getKey().length + pair.getValue().value().length;
                longBuffer.putLong(size);
                writer.write(longBuffer.array());
                longBuffer.clear();
            }
        }
    }

    private BaseEntry<byte[]> FindInFiles(byte[] key) throws IOException {
        for (long i = filesCount - 1; i >= 0; i--) {
            Path currentFile = config.basePath().resolve(FILE_NAME + i + FILE_EXTENTION);
            Path currentIndexFile = config.basePath().resolve(FILE_INDEX_NAME + i + FILE_INDEX_EXTENTION);
            long low = 0;
            long high = IndexSize(currentIndexFile);
            long mid = (low + high) / 2;
            while (low <= high) {
                BaseEntry<byte[]> current = GetCurrent(mid, currentFile, currentIndexFile);
                int compare = Arrays.compare(key, current.key());
                if (compare > 0) {
                    low = mid + 1;
                } else if (compare < 0) {
                    high = mid - 1;
                } else {
                    return current;
                }
                mid = (low + high) / 2;
            }
        }
        return null;
    }

    private static long IndexSize(Path indexPath) throws IOException {
        long size;
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        try (FileInputStream fis = new FileInputStream(String.valueOf(indexPath));
             BufferedInputStream reader = new BufferedInputStream(fis, bufferSize)) {
            buffer.put(reader.readNBytes(Long.BYTES));
            buffer.flip();
            size = buffer.getLong();
        }
        return size;
    }

    private static BaseEntry<byte[]> GetCurrent(long mid, Path path, Path indexPath) throws IOException {
        long position;
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        try (FileInputStream fis = new FileInputStream(String.valueOf(indexPath))) {
            fis.skipNBytes((mid + 1) * Long.BYTES);
            buffer.put(fis.readNBytes(Long.BYTES));
            buffer.flip();
            position = buffer.getLong();
        }
        byte[] currentKey;
        byte[] currentValue;
        ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
        try (FileInputStream fis = new FileInputStream(String.valueOf(path))) {
            fis.skipNBytes(position);
            intBuffer.put(fis.readNBytes(Integer.BYTES));
            intBuffer.flip();
            int keyLength = intBuffer.getInt();
            intBuffer.clear();
            currentKey = fis.readNBytes(keyLength);
            intBuffer.put(fis.readNBytes(Integer.BYTES));
            intBuffer.flip();
            int valueLength = intBuffer.getInt();
            currentValue = fis.readNBytes(valueLength);
        }
        return new BaseEntry<>(currentKey, currentValue);
    }
}
