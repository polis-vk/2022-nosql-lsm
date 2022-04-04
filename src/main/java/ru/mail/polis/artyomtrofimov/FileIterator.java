package ru.mail.polis.artyomtrofimov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileIterator implements Iterator<Entry<String>> {
    private final String to;
    private final RandomAccessFile raf;
    private Entry<String> nextEntry;
    private final long fileLength;

    public FileIterator(Path basePath, String name, String from, String to) throws IOException {
        this.to = to;
        raf = new RandomAccessFile(basePath.resolve(name + InMemoryDao.DATA_EXT).toString(), "r");
        fileLength = raf.length();
        nextEntry = Utils.findCeilEntry(raf, from, basePath.resolve(name + InMemoryDao.INDEX_EXT));
    }

    @Override
    public boolean hasNext() {
        boolean hasNext;
        try {
            hasNext = (to == null && nextEntry != null) || (nextEntry != null && nextEntry.key().compareTo(to) < 0);
            if (!hasNext) {
                raf.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return hasNext;
    }

    @Override
    public Entry<String> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Entry<String> retval = nextEntry;
        try {
            if (raf.getFilePointer() < fileLength) {
                byte tombstone = raf.readByte();
                String currentKey = raf.readUTF();
                String currentValue = tombstone < 0 ? null : raf.readUTF();
                nextEntry = new BaseEntry<>(currentKey, currentValue);
            } else {
                nextEntry = null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return retval;
    }
}
