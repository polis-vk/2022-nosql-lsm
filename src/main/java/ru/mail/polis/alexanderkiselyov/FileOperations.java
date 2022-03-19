package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

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
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public record FileOperations(Config config, long filesCount, String fileName) {
    private static final int BUFFER_SIZE = 2000 * Character.BYTES;
    private static final String FILE_EXTENSION = ".txt";
    private static final String FILE_INDEX_NAME = "myIndex";
    private static final String FILE_INDEX_EXTENSION = ".txt";

    void saveData(NavigableMap<byte[], BaseEntry<byte[]>> sortedPairs) throws IOException {
        Path newFilePath = config.basePath().resolve(fileName + filesCount + FILE_EXTENSION);
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

    void saveIndexes(NavigableMap<byte[], BaseEntry<byte[]>> sortedPairs) throws IOException {
        Path newIndexPath = config.basePath().resolve(FILE_INDEX_NAME + filesCount + FILE_INDEX_EXTENSION);
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

    BaseEntry<byte[]> findInFiles(byte[] key) throws IOException {
        for (long i = filesCount - 1; i >= 0; i--) {
            Path currentFile = config.basePath().resolve(fileName + i + FILE_EXTENSION);
            Path currentIndexFile = config.basePath().resolve(FILE_INDEX_NAME + i + FILE_INDEX_EXTENSION);
            long low = 0;
            long high = indexSize(currentIndexFile) - 1;
            long mid = (low + high) / 2;
            while (low <= high) {
                BaseEntry<byte[]> current = getCurrent(mid, currentFile, currentIndexFile);
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

    private BaseEntry<byte[]> getCurrent(long mid, Path path, Path indexPath) throws IOException {
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
            if (valueLength == -1) {
                return new BaseEntry<>(currentKey, null);
            }
            currentValue = fis.readNBytes(valueLength);
        }
        return new BaseEntry<>(currentKey, currentValue);
    }

    Iterator<BaseEntry<byte[]>> diskIterator(byte[] from, byte[] to) throws IOException {
        NavigableMap<byte[], BaseEntry<byte[]>> diskPairs = new ConcurrentSkipListMap<>(Arrays::compare);
        for (long i = filesCount - 1; i >= 0; i--) {
            Path currentFile = config.basePath().resolve(fileName + i + FILE_EXTENSION);
            Path currentIndexFile = config.basePath().resolve(FILE_INDEX_NAME + i + FILE_INDEX_EXTENSION);
            addPairsFromDisk(from, to, diskPairs, currentFile, currentIndexFile);
        }
        return diskPairs.values().iterator();
    }

    private void addPairsFromDisk(byte[] from, byte[] to, NavigableMap<byte[], BaseEntry<byte[]>> diskPairs,
                                  Path currentFile, Path currentIndexFile) throws IOException {
        long low = 0;
        long high = indexSize(currentIndexFile) - 1;
        long fromIndex = low;
        long toIndex = high;
        long mid = (low + high) / 2;
        while (low <= high) {
            BaseEntry<byte[]> current = getCurrent(mid, currentFile, currentIndexFile);
            int compare = Arrays.compare(from, current.key());
            if (compare > 0) {
                low = mid + 1;
                fromIndex = low;
            } else if (compare < 0) {
                high = mid - 1;
            } else {
                fromIndex = mid;
                break;
            }
            mid = (low + high) / 2;
        }
        if (to != null) {
            low = fromIndex;
            high = toIndex;
            mid = (low + high) / 2;
            while (low <= high) {
                BaseEntry<byte[]> current = getCurrent(mid, currentFile, currentIndexFile);
                int compare = Arrays.compare(to, current.key());
                if (compare > 0) {
                    low = mid + 1;
                } else if (compare < 0) {
                    high = mid - 1;
                    toIndex = high;
                } else {
                    toIndex = mid - 1;
                    break;
                }
                mid = (low + high) / 2;
            }
        }
        putPairs(fromIndex, toIndex, currentFile, currentIndexFile, diskPairs);
    }

    private void putPairs(long fromIndex, long toIndex, Path currentFile, Path currentIndexFile,
                          NavigableMap<byte[], BaseEntry<byte[]>> diskPairs) throws IOException {
        for (long j = fromIndex; j <= toIndex; j++) {
            BaseEntry<byte[]> current = getCurrent(j, currentFile, currentIndexFile);
            diskPairs.putIfAbsent(current.key(), current);
        }
    }
}
