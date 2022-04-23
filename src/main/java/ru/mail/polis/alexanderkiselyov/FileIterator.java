package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileIterator implements Iterator<BaseEntry<byte[]>>, Closeable {

    private final FileChannel channelTable;
    private final FileChannel channelIndex;
    private final RandomAccessFile rafTable;
    private final RandomAccessFile rafIndex;
    private long pos;
    private final long to;
    private final FileOperations fileOperations;
    private static final byte[] VERY_FIRST_KEY = new byte[]{};

    public FileIterator(Path ssTable, Path ssIndex, byte[] from, byte[] to, long indexSize,
                        FileOperations fileOperations) throws IOException {
        rafTable = new RandomAccessFile(String.valueOf(ssTable), "r");
        rafIndex = new RandomAccessFile(String.valueOf(ssIndex), "r");
        channelTable = rafTable.getChannel();
        channelIndex = rafIndex.getChannel();
        pos = from == VERY_FIRST_KEY ? 0 : fileOperations.getEntryIndex(channelTable, channelIndex, from, indexSize);
        this.to = to == null ? indexSize : fileOperations.getEntryIndex(channelTable, channelIndex, to, indexSize);
        this.fileOperations = fileOperations;
    }

    @Override
    public boolean hasNext() {
        return pos < to;
    }

    @Override
    public BaseEntry<byte[]> next() {
        BaseEntry<byte[]> entry;
        try {
            entry = fileOperations.getCurrent(pos, channelTable, channelIndex);
        } catch (IOException e) {
            throw new NoSuchElementException("There is no next element!", e);
        }
        pos++;
        return entry;
    }

    @Override
    public void close() throws IOException {
        channelTable.close();
        channelIndex.close();
        rafTable.close();
        rafIndex.close();
    }
}
