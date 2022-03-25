package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

public class NewFileIterator implements Iterator<BaseEntry<String>> {
    private final DaoFile daoFile;
    private final String to;
    private int entryToRead;
    private BaseEntry<String> next;
    private final FileChannel channel;
    private final ByteBuffer buffer;

    public NewFileIterator(String from, String to, DaoFile daoFile) throws IOException {
        this.daoFile = daoFile;
        this.channel = daoFile.getChannel();
        this.buffer = ByteBuffer.allocate(daoFile.maxEntrySize());
        this.to = to;
        next = from == null ? readEntry() : findValidClosest(from, to);
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

    private BaseEntry<String> findValidClosest(String from, String to) throws IOException {
        int left = 0;
        int right = daoFile.getLastIndex();
        int validEntryIndex = -1;
        while (left <= right) {
            int middle = (right - left) / 2 + left;
            daoFile.fillBufferWithEntry(buffer, middle);
            String key = daoFile.readKeyFromBuffer(buffer);

            int comparison = from.compareTo(key);
            if (comparison < 0) {
                validEntryIndex = middle;
                right = middle - 1;
            } else if (comparison > 0) {
                left = middle + 1;
            } else {
                String value = daoFile.readValueFromBuffer(buffer, middle);
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
        daoFile.fillBufferWithEntry(buffer, entryToRead);
        String key = daoFile.readKeyFromBuffer(buffer);
        if (to != null && key.compareTo(to) >= 0) {
            return null;
        }
        String value = daoFile.readValueFromBuffer(buffer, entryToRead);
        entryToRead++;
        return new BaseEntry<>(key, value);
    }
}
