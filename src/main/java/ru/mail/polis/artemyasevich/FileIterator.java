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
        this.entryToRead = from == null ? 0 : StringDao.getEntryIndex(from, daoFile, buffer);
        this.next = readEntry();
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

    private BaseEntry<String> readEntry() throws IOException {
        if (daoFile.getOffset(entryToRead) == daoFile.sizeOfFile()) {
            return null;
        }
        BaseEntry<String> entry = StringDao.readEntry(entryToRead, daoFile, buffer);
        if (to != null && entry.key().compareTo(to) >= 0) {
            return null;
        }
        entryToRead++;
        return entry;
    }
}
