package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileIterator implements Iterator<BaseEntry<byte[]>> {

    private final FileChannel cTable;
    private final FileChannel cIndex;
    private long pos;
    private final long to;

    public FileIterator(Path ssTable, Path ssIndex, byte[] from, byte[] to, long indexSize) throws IOException {
        RandomAccessFile rafTable = new RandomAccessFile(String.valueOf(ssTable), "r");
        RandomAccessFile rafIndex = new RandomAccessFile(String.valueOf(ssIndex), "r");
        cTable = rafTable.getChannel();
        cIndex = rafIndex.getChannel();
        pos = from == null ? 0 : FileOperations.getEntryIndex(cTable, cIndex, from, indexSize);
        this.to = to == null ? indexSize : FileOperations.getEntryIndex(cTable, cIndex, to, indexSize);
    }

    @Override
    public boolean hasNext() {
        return pos < to;
    }

    @Override
    public BaseEntry<byte[]> next() {
        BaseEntry<byte[]> entry;
        try {
            entry = FileOperations.getCurrent(pos, cTable, cIndex);
        } catch (IOException e) {
            throw new NoSuchElementException("There is no next element!", e);
        }
        pos++;
        return entry;
    }

    public FileChannel getCTable() {
        return cTable;
    }

    public FileChannel getCIndex() {
        return cIndex;
    }
}
