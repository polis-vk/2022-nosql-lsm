package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MergeIterator implements Iterator<BaseEntry<String>> {
    private static final String DATA_FILE = "data";
    private static final String FILE_EXTENSION = ".txt";
    private final int numberOfFiles;
    private final List<RandomAccessFile> readers;
    private final List<BaseEntry<String>> currents;
    private final List<long[]> offsets;
    private final int[] nextEntriesToRead;
    private final Iterator<BaseEntry<String>> dataMapIterator;
    private final String to;
    private BaseEntry<String> next;

    public MergeIterator(String from, String to, int numberOfFiles, Iterator<BaseEntry<String>> dataMapIterator,
                         Path pathToDirectory, List<long[]> offsets) throws IOException {
        this.numberOfFiles = numberOfFiles;
        this.to = to;
        this.dataMapIterator = dataMapIterator;
        this.offsets = offsets;
        this.nextEntriesToRead = new int[numberOfFiles];
        this.readers = new ArrayList<>(this.numberOfFiles);
        this.currents = new ArrayList<>(this.numberOfFiles + 1);
        for (int fileNumber = 0; fileNumber < this.numberOfFiles; fileNumber++) {
            Path path = pathToDirectory.resolve(DATA_FILE + fileNumber + FILE_EXTENSION);
            RandomAccessFile reader = new RandomAccessFile(path.toFile(), "r");
            readers.add(reader);
            BaseEntry<String> entry = from == null ? readEntry(fileNumber) : findValidClosest(from, to, fileNumber);
            currents.add(entry);
            if (entry == null) {
                reader.close();
                readers.set(fileNumber, null);
            }
        }
        currents.add(this.dataMapIterator.hasNext() ? this.dataMapIterator.next() : null);
        setNext();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public BaseEntry<String> next() {
        BaseEntry<String> nextToGive = next;
        try {
            skipEntriesWithKey(next.key());
            setNext();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return nextToGive;
    }

    private void setNext() throws IOException {
        next = getNext();
        while (next != null && next.value() == null) {
            skipEntriesWithKey(next.key());
            next = getNext();
        }
    }

    private BaseEntry<String> getNext() {
        String minKey = null;
        int minI = 0;
        for (int i = numberOfFiles; i >= 0; i--) {
            if (currents.get(i) != null) {
                minKey = currents.get(i).key();
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

        return currents.get(minI);
    }

    private void skipEntriesWithKey(String keyToSkip) throws IOException {
        for (int i = 0; i < currents.size(); i++) {
            BaseEntry<String> entry = currents.get(i);
            if (entry == null) {
                continue;
            }
            if (entry.key().equals(keyToSkip)) {
                currents.set(i, readEntry(i));
            }
        }
    }

    private BaseEntry<String> findValidClosest(String from, String to, int fileNumber) throws IOException {
        RandomAccessFile reader = readers.get(fileNumber);
        long[] fileOffsets = offsets.get(fileNumber);
        int left = 0;
        int middle;
        int right = fileOffsets.length - 2;
        String validKey = null;
        String validValue = null;
        int validEntryIndex = 0;
        while (left <= right) {
            middle = (right - left) / 2 + left;
            reader.seek(fileOffsets[middle]);
            String key = reader.readUTF();
            int comparison = from.compareTo(key);
            if (comparison <= 0) {
                String value = reader.getFilePointer() == fileOffsets[middle + 1] ? null : reader.readUTF();
                if (comparison == 0) {
                    nextEntriesToRead[fileNumber] = middle + 1;
                    return new BaseEntry<>(key, value);
                } else {
                    right = middle - 1;
                    validKey = key;
                    validValue = value;
                    validEntryIndex = middle;
                }
            } else {
                left = middle + 1;
            }
        }
        if (validKey == null) {
            return null;
        }
        nextEntriesToRead[fileNumber] = validEntryIndex + 1;
        return to != null && validKey.compareTo(to) >= 0 ? null : new BaseEntry<>(validKey, validValue);
    }

    private BaseEntry<String> readEntry(int fileNumber) throws IOException {
        if (fileNumber == numberOfFiles) {
            return dataMapIterator.hasNext() ? dataMapIterator.next() : null;
        }
        RandomAccessFile reader = readers.get(fileNumber);
        int entryToRead = nextEntriesToRead[fileNumber];
        long[] fileOffsets = offsets.get(fileNumber);
        reader.seek(fileOffsets[entryToRead]);
        if (reader.getFilePointer() == fileOffsets[fileOffsets.length - 1]) {
            reader.close();
            return null;
        }
        String key = reader.readUTF();
        if (to != null && key.compareTo(to) >= 0) {
            reader.close();
            return null;
        }
        String value = reader.getFilePointer() == fileOffsets[entryToRead + 1] ? null : reader.readUTF();
        nextEntriesToRead[fileNumber]++;
        return new BaseEntry<>(key, value);
    }
}
