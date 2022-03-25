package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.util.Iterator;

public class FileIterator implements Iterator<BaseEntry<String>> {
    private final DaoFile daoFile;
    private final RandomAccessFile reader;
    private final String to;
    private int entryToRead;
    private BaseEntry<String> next;

    public FileIterator(String from, String to, DaoFile daoFile) throws IOException {
        this.daoFile = daoFile;
        //this.reader = new RandomAccessFile(daoFile.getPathToFile().toFile(), "r");
        this.reader = null;//daoFile.getReader();
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
        int right = daoFile.getLastIndex();
        String validKey = null;
        String validValue = null;
        int validEntryIndex = 0;
        while (left <= right) {
            int middle = (right - left) / 2 + left;
            reader.seek(daoFile.getOffset(middle));
            String key = reader.readUTF();
            int comparison = from.compareTo(key);
            if (comparison <= 0) {
                String value = reader.getFilePointer() == daoFile.getOffset(middle + 1) ? null : reader.readUTF();
                if (comparison < 0) {
                    right = middle - 1;
                    validKey = key;
                    validValue = value;
                    validEntryIndex = middle;
                } else {
                    entryToRead = middle + 1;
                    return new BaseEntry<>(key, value);
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
        reader.seek(daoFile.getOffset(entryToRead));
        if (reader.getFilePointer() == daoFile.getOffset(daoFile.getLastIndex() + 1)) {
            reader.close();
            return null;
        }
        String key = reader.readUTF();
        if (to != null && key.compareTo(to) >= 0) {
            reader.close();
            return null;
        }
        String value = reader.getFilePointer() == daoFile.getOffset(entryToRead + 1) ? null : reader.readUTF();
        entryToRead++;
        return new BaseEntry<>(key, value);
    }

}
