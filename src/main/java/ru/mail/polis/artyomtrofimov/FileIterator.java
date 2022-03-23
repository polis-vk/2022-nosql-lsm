package ru.mail.polis.artyomtrofimov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileIterator implements Iterator<Entry<String>> {
    private final String to;
    private RandomAccessFile raf;
    private Entry<String> nextEntry;

    public FileIterator(Path basePath, String name, String from, String to) throws IOException {
        this.to = to;
        try {
            raf = new RandomAccessFile(basePath.resolve(name + InMemoryDao.DATA_EXT).toString(), "r");
            nextEntry = Utils.findCeilEntry(raf, from, basePath.resolve(name + InMemoryDao.INDEX_EXT));
        } catch (FileNotFoundException | EOFException e) {
            nextEntry = null;
        }
    }

    @Override
    public boolean hasNext() {
        boolean retval = (to == null && nextEntry != null) || (nextEntry != null && nextEntry.key().compareTo(to) < 0);
        if (!retval) {
            try {
                raf.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return retval;
    }

    @Override
    public Entry<String> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Entry<String> retval = nextEntry;
        try {
            byte tombstone = raf.readByte();
            String currentKey = raf.readUTF();
            String currentValue = tombstone < 0 ? null : raf.readUTF();
            nextEntry = new BaseEntry<>(currentKey, currentValue);
        } catch (EOFException e) {
            nextEntry = null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return retval;
    }
}
