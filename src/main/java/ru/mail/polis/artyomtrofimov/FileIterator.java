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
    private final Path basePath;
    private final String from;
    private final String to;
    private RandomAccessFile raf;
    private long lastPos;
    private Entry<String> nextEntry;

    public FileIterator(Path basePath, String name, String from, String to) throws IOException {
        this.from = from;
        this.to = to;
        this.basePath = basePath;
        try {
            raf = new RandomAccessFile(basePath.resolve(name + InMemoryDao.DATA_EXT).toString(), "r");
            findFloorEntry(name);
        } catch (FileNotFoundException | EOFException e) {
            nextEntry = null;
        }
    }

    private void findFloorEntry(String name) throws IOException {
        String filename = name + InMemoryDao.INDEX_EXT;
        try (RandomAccessFile index = new RandomAccessFile(basePath.resolve(filename).toString(), "r")) {
            raf.seek(0);
            int size = raf.readInt();
            long left = -1;
            long right = size;
            long mid;
            while (left < right - 1) {
                mid = left + (right - left) / 2;
                index.seek(mid * Long.BYTES);
                raf.seek(index.readLong());
                byte tombstone = raf.readByte();
                String currentKey = raf.readUTF();
                String currentValue = tombstone < 0 ? null : raf.readUTF();
                int keyComparing = currentKey.compareTo(from);
                if (keyComparing == 0) {
                    lastPos = raf.getFilePointer();
                    this.nextEntry = new BaseEntry<>(currentKey, currentValue);
                    break;
                } else if (keyComparing > 0) {
                    lastPos = raf.getFilePointer();
                    this.nextEntry = new BaseEntry<>(currentKey, currentValue);
                    right = mid;
                } else {
                    left = mid;
                }
            }
            raf.seek(lastPos);
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
