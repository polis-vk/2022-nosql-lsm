package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class MergeIterator implements Iterator<BaseEntry<String>> {
    private static final String DATA_FILE = "data";
    private static final String FILE_EXTENSION = ".txt";
    private final int numberOfFiles;
    private final List<RandomAccessFile> readers;
    private final List<BaseEntry<String>> currents;
    private final List<long[]> offsets;
    private final Iterator<BaseEntry<String>> dataMapIterator;
    private final String to;

    public MergeIterator(String from, String to, int numberOfFiles, Iterator<BaseEntry<String>> dataMapIterator,
                         Path pathToDirectory, List<long[]> offsets) throws IOException {
        this.numberOfFiles = numberOfFiles;
        this.to = to;
        this.dataMapIterator = dataMapIterator;
        this.offsets = offsets;
        readers = new ArrayList<>(this.numberOfFiles);
        currents = new ArrayList<>(this.numberOfFiles + 1);
        for (int fileNumber = 0; fileNumber < this.numberOfFiles; fileNumber++) {
            Path path = pathToDirectory.resolve(DATA_FILE + fileNumber + FILE_EXTENSION);
            RandomAccessFile reader = new RandomAccessFile(path.toFile(), "r");
            readers.add(reader);
            if (from == null) {
                currents.add(readEntry(fileNumber));
                continue;
            }
            BaseEntry<String> closest = findValidClosest(from, to, reader, fileNumber);
            currents.add(closest);
            if (closest == null) {
                reader.close();
                readers.set(fileNumber, null);
            }
        }
        currents.add(this.dataMapIterator.hasNext() ? this.dataMapIterator.next() : null);
    }

    @Override
    public boolean hasNext() {
        return currents.stream().anyMatch(Objects::nonNull);
    }

    @Override
    public BaseEntry<String> next() {
        String minKey = "";
        int minI = 0;
        for (int i = numberOfFiles; i >= 0; i--) {
            BaseEntry<String> current = currents.get(i);
            if (current != null) {
                minKey = current.key();
                minI = i;
                break;
            }
        }

        for (int i = minI - 1; i >= 0; i--) {
            BaseEntry<String> current = currents.get(i);
            if (current == null) {
                continue;
            }
            if (current.key().compareTo(minKey) < 0) {
                minKey = current.key();
                minI = i;
            }
        }
        BaseEntry<String> next = currents.get(minI);
        try {
            currents.set(minI, readEntry(minI));
            for (int i = 0; i < numberOfFiles + 1; i++) {
                BaseEntry<String> current = currents.get(i);
                if (current == null) {
                    continue;
                }
                if (current.key().equals(minKey)) {
                    currents.set(i, readEntry(i));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return next;
    }

    private BaseEntry<String> findValidClosest(String from, String to,
                                               RandomAccessFile reader, int fileNumber) throws IOException {
        int left = 0;
        int middle;
        int right = offsets.get(fileNumber).length - 2;
        String goodKey = null;
        String goodValue = null;
        while (left <= right) {
            middle = (right - left) / 2 + left;
            long pos = offsets.get(fileNumber)[middle];
            reader.seek(pos);
            String key = reader.readUTF();
            int comparison = from.compareTo(key);
            if (comparison <= 0) {
                String value = reader.readUTF();
                if (comparison == 0) {
                    return new BaseEntry<>(key, value);
                } else {
                    right = middle - 1;
                    goodKey = key;
                    goodValue = value;
                }
            } else {
                left = middle + 1;
            }
        }
        if (goodKey == null) {
            return null;
        }
        return to != null && goodKey.compareTo(to) >= 0 ? null : new BaseEntry<>(goodKey, goodValue);
    }

    private BaseEntry<String> readEntry(int fileNumber) throws IOException {
        if (fileNumber == numberOfFiles) {
            return dataMapIterator.hasNext() ? dataMapIterator.next() : null;
        }
        RandomAccessFile reader = readers.get(fileNumber);
        long[] curOffsets = offsets.get(fileNumber);
        if (reader.getFilePointer() == curOffsets[curOffsets.length - 1]) {
            reader.close();
            return null;
        }
        String key = reader.readUTF();
        if (to != null && key.compareTo(to) >= 0) {
            reader.close();
            return null;
        }
        String value = reader.readUTF();
        return new BaseEntry<>(key, value);
    }
}
