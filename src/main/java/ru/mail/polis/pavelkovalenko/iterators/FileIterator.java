package ru.mail.polis.pavelkovalenko.iterators;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;
import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import ru.mail.polis.pavelkovalenko.comparators.EntryComparator;
import ru.mail.polis.pavelkovalenko.Serializer;
import ru.mail.polis.pavelkovalenko.utils.Utils;

public class FileIterator implements Iterator<Entry<ByteBuffer>>, Closeable {

    private final RandomAccessFile dataFile;
    private final RandomAccessFile indexesFile;
    private final ByteBuffer from;
    private final ByteBuffer to;
    private final Entry<ByteBuffer> toEntry;
    private Entry<ByteBuffer> current;

    public FileIterator(Path pathToDataFile, Path pathToIndexesFile, ByteBuffer from, ByteBuffer to)
            throws IOException {
        this.dataFile = new RandomAccessFile(pathToDataFile.toString(), "r");
        this.indexesFile = new RandomAccessFile(pathToIndexesFile.toString(), "r");
        this.from = from;
        this.to = to;
        toEntry = new BaseEntry<>(to, to);
        current = binarySearchInFile();

        if (current == null) {
            close();
        }
    }

    @Override
    public boolean hasNext() {
        try {
            boolean hasNext = dataExists() && canContinue();
            if (!hasNext) {
                close();
            }
            return hasNext;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Entry<ByteBuffer> next() {
        try {
            Entry<ByteBuffer> peek = peek();
            current = null;
            if (!dataExists() || !canContinue()) {
                close();
            }
            return peek;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        dataFile.close();
        indexesFile.close();
    }

    private Entry<ByteBuffer> peek() throws IOException {
        if (current == null && !isEOFReached()) {
            current = Serializer.readEntry(dataFile, indexesFile);
        }
        return current;
    }

    private boolean dataExists() throws IOException {
        return peek() != null;
    }

    private boolean canContinue() throws IOException {
        return (to == null && peek() != null)
                || (peek() != null && EntryComparator.INSTANSE.compare(peek(), toEntry) < 0);
    }

    private boolean isEOFReached() throws IOException {
        return !dataFile.getChannel().isOpen() || dataFile.getFilePointer() >= dataFile.length();
    }

    private Entry<ByteBuffer> binarySearchInFile() throws IOException {
        if (!hasNext() || isFromOutOfBound()) {
            return null;
        }
        Entry<ByteBuffer> ceilEntry = getLast();

        long a = 0;
        long b = getIndexesFileLength() / Utils.INDEX_OFFSET;
        long lastDataFileOffset = 0;
        long lastIndexesFileOffset = 0;

        while (b - a >= 1) {
            long c = (b + a) / 2;
            setIndexesFileOffset(c * Utils.INDEX_OFFSET);
            Entry<ByteBuffer> curEntry = Serializer.readEntry(dataFile, indexesFile);

            if (curEntry.key().compareTo(from) >= 0 && EntryComparator.INSTANSE.compare(curEntry, ceilEntry) <= 0) {
                ceilEntry = curEntry;
                lastIndexesFileOffset = getIndexesFileOffset();
                lastDataFileOffset = getDataFileOffset();
            }

            int compare = curEntry.key().compareTo(from);
            if (compare < 0) {
                if (b - a <= 1) {
                    break;
                }
                a = c;
            } else if (compare == 0) {
                ceilEntry = curEntry;
                break;
            } else {
                if (b - a <= 1) {
                    break;
                }
                b = c;
            }
        }

        setIndexesFileOffset(lastIndexesFileOffset);
        setDataFileOffset(lastDataFileOffset);
        return ceilEntry;
    }

    private Entry<ByteBuffer> getLast() throws IOException {
        setIndexesFileOffset(getIndexesFileLength() - Utils.INDEX_OFFSET);
        Entry<ByteBuffer> last = Serializer.readEntry(dataFile, indexesFile);
        setIndexesFileOffset(0);
        return last;
    }

    private boolean isFromOutOfBound() throws IOException {
        return from.compareTo(getLast().key()) > 0;
    }

    private long getIndexesFileLength() throws IOException {
        return indexesFile.length();
    }

    private void setDataFileOffset(long dataFileOffset) throws IOException {
        dataFile.seek(dataFileOffset);
    }

    private long getIndexesFileOffset() throws IOException {
        return indexesFile.getFilePointer();
    }

    private long getDataFileOffset() throws IOException {
        return dataFile.getFilePointer();
    }

    private void setIndexesFileOffset(long indexesFileOffset) throws IOException {
        indexesFile.seek(indexesFileOffset);
    }

}
