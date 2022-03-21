package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;

public class FileIterator implements Iterator<BaseEntry<String>> {
    private final long[] offsets;
    private final RandomAccessFile reader;
    private final String to;
    private int entryToRead;
    private BaseEntry<String> next;

    public FileIterator(String from, String to, Path pathToFile, long[] offsets) throws IOException {
        this.offsets = offsets;
        this.reader = new RandomAccessFile(pathToFile.toFile(), "r");
        this.to = to;
        BaseEntry<String> entry = from == null ? readEntry() : findValidClosest(from, to);
        if (entry == null) {
            reader.close();
        }
        next = entry;
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public BaseEntry<String> next() {
        BaseEntry<String> nextToGive = next;
        try {
            next = readEntry();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return nextToGive;
    }

    private BaseEntry<String> findValidClosest(String from, String to) throws IOException {
        int left = 0;
        int right = offsets.length - 2;
        String validKey = null;
        String validValue = null;
        int validEntryIndex = 0;
        while (left <= right) {
            int middle = (right - left) / 2 + left;
            reader.seek(offsets[middle]);
            String key = reader.readUTF();
            int comparison = from.compareTo(key);
            if (comparison <= 0) {
                String value = reader.getFilePointer() == offsets[middle + 1] ? null : reader.readUTF();
                if (comparison == 0) {
                    entryToRead = middle + 1;
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
        entryToRead = validEntryIndex + 1;
        return to != null && validKey.compareTo(to) >= 0 ? null : new BaseEntry<>(validKey, validValue);
    }

    private BaseEntry<String> readEntry() throws IOException {
        reader.seek(offsets[entryToRead]);
        if (reader.getFilePointer() == offsets[offsets.length - 1]) {
            reader.close();
            return null;
        }
        String key = reader.readUTF();
        if (to != null && key.compareTo(to) >= 0) {
            reader.close();
            return null;
        }
        String value = reader.getFilePointer() == offsets[entryToRead + 1] ? null : reader.readUTF();
        entryToRead++;
        return new BaseEntry<>(key, value);
    }
}
