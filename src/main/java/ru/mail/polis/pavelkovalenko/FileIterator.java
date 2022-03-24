package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;
import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;

public class FileIterator implements Iterator<Entry<ByteBuffer>>, Closeable {

    private final RandomAccessFile dataFile;
    private final RandomAccessFile indexesFile;
    private final ByteBuffer from;
    private final ByteBuffer to;
    private final Entry<ByteBuffer> toEntry;
    private Entry<ByteBuffer> lastEntry = Utils.EMPTY_ENTRY;

    public FileIterator(Path pathToDataFile, Path pathToIndexesFile, ByteBuffer from, ByteBuffer to)
            throws IOException {
        this.dataFile = new RandomAccessFile(pathToDataFile.toString(), "r");
        this.indexesFile = new RandomAccessFile(pathToIndexesFile.toString(), "r");
        this.from = from;
        this.to = to;
        toEntry = new BaseEntry<>(to, to);
        setCeilFilePointer();
    }

    @Override
    public boolean hasNext() {
        try {
            boolean hasNext = (dataExists() || (!dataExists() && lastEntry != null)) && canContinue();
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
        if (lastEntry == null) {
            throw new IndexOutOfBoundsException("Out-of-bound file iteration");
        }

        Entry<ByteBuffer> res = lastEntry;

        try {
            if (!dataExists()) {
                lastEntry = null;
                return res;
            }
            lastEntry = readEntry();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return res;
    }

    @Override
    public void close() throws IOException {
        dataFile.close();
        indexesFile.close();
    }

    private int readDataFileOffset() throws IOException {
        int dataFileOffset = indexesFile.readInt();
        indexesFile.readLine();
        return dataFileOffset;
    }

    private void setCeilFilePointer() throws IOException {
        lastEntry = binarySearchInFile();
    }

    private boolean dataExists() throws IOException {
        return dataFile.getChannel().isOpen() && dataFile.getFilePointer() < dataFile.length();
    }

    private boolean canContinue() {
        return (to == null && lastEntry != null)
                || (lastEntry != null && Utils.entryComparator.compare(lastEntry, toEntry) < 0);
    }

    private Entry<ByteBuffer> readEntry() throws IOException {
        int dataFileOffset = readDataFileOffset();
        dataFile.seek(dataFileOffset);
        byte tombstone = readByte(dataFile);
        ByteBuffer key = readByteBuffer(dataFile);
        ByteBuffer value = Utils.isTombstone(tombstone) ? null : readByteBuffer(dataFile);
        Entry<ByteBuffer> entry = new BaseEntry<>(key, value);
        dataFile.readLine();
        return entry;
    }

    private Entry<ByteBuffer> binarySearchInFile() throws IOException {
        if (isGettingAllHeadData()) {
            return null;
        }
        if (!hasNext()) {
            return null;
        }

        long a = 0;
        long b = getIndexesFileLength() / Utils.OFFSET_VALUES_DISTANCE;
        Entry<ByteBuffer> ceilEntry = new BaseEntry<>(to, Utils.EMPTY_BYTEBUFFER);
        long lastDataFileOffset = 0;
        long lastIndexesFileOffset = 0;

        while (b - a >= 1) {
            long c = (b + a) / 2;
            setIndexesFileOffset(c * Utils.OFFSET_VALUES_DISTANCE);
            Entry<ByteBuffer> curEntry = readEntry();

            if (curEntry.key().compareTo(from) >= 0 && Utils.entryComparator.compare(curEntry, ceilEntry) < 0) {
                ceilEntry = curEntry;
                lastDataFileOffset = getDataFileOffset();
                lastIndexesFileOffset = getIndexesFileOffset();
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

        if (to == null && ceilEntry.key() == null || ceilEntry.key().equals(to)) {
            return null;
        }

        setIndexesFileOffset(lastIndexesFileOffset);
        setDataFileOffset(lastDataFileOffset);
        return ceilEntry;
    }

    private ByteBuffer readByteBuffer(RandomAccessFile dataFile) throws IOException {
        int bbSize = dataFile.readInt();
        ByteBuffer bb = ByteBuffer.allocate(bbSize);
        dataFile.getChannel().read(bb);
        bb.rewind();
        return bb;
    }

    private boolean isGettingAllHeadData() {
        return this.from == null;
    }

    private byte readByte(RandomAccessFile dataFile) throws IOException {
        return dataFile.readByte();
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
