package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class FileIterator implements Iterator<BaseEntry<String>> {
    private final DaoFile daoFile;
    private final String to;
    private final ByteBuffer buffer;
    private int entryToRead;
    private BaseEntry<String> next;

    public FileIterator(String from, String to, DaoFile daoFile, ByteBuffer buffer) throws IOException {
        this.daoFile = daoFile;
        this.buffer = buffer;
        this.to = to;
        this.next = from == null ? readEntry() : findValidClosest(from);
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
            e.printStackTrace();
        }
        return nextToGive;
    }

    private BaseEntry<String> findValidClosest(String from) throws IOException {
        int left = 0;
        int right = daoFile.getLastIndex();
        int validEntryIndex = -1;
        while (left <= right) {
            int middle = (right - left) / 2 + left;
            InMemoryDao.fillBufferWithEntry(daoFile, buffer, middle);
            String key = InMemoryDao.readKeyFromBuffer(buffer);

            int comparison = from.compareTo(key);
            if (comparison < 0) {
                validEntryIndex = middle;
                right = middle - 1;
            } else if (comparison > 0) {
                left = middle + 1;
            } else {
                String value = InMemoryDao.readValueFromBuffer(daoFile, buffer, middle);
                entryToRead = middle + 1;
                return new BaseEntry<>(key, value);
            }
        }
        if (validEntryIndex == -1) {
            return null;
        }
        entryToRead = validEntryIndex;
        return readEntry();
    }

    private BaseEntry<String> readEntry() throws IOException {
        if (daoFile.getOffset(entryToRead) == daoFile.size()) {
            return null;
        }
        InMemoryDao.fillBufferWithEntry(daoFile, buffer, entryToRead);
        String key = InMemoryDao.readKeyFromBuffer(buffer);
        if (to != null && key.compareTo(to) >= 0) {
            return null;
        }
        String value = InMemoryDao.readValueFromBuffer(daoFile, buffer, entryToRead);
        entryToRead++;
        return new BaseEntry<>(key, value);
    }
}
