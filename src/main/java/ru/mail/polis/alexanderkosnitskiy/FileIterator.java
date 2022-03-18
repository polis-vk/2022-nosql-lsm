package ru.mail.polis.alexanderkosnitskiy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Iterator;

import ru.mail.polis.BaseEntry;

public class FileIterator implements Iterator<BaseEntry<ByteBuffer>> {
    private final DaoReader reader;
    private BaseEntry<ByteBuffer> nextValue;
    private final ByteBuffer to;

    public FileIterator(Path path, Path indexPath, ByteBuffer initialKey, ByteBuffer to) {
        try {
            reader = new DaoReader(path, indexPath);
        } catch (NoSuchFileException e) {
            throw new IllegalArgumentException();
        }
        if(initialKey != null) {
            nextValue = reader.nonPreciseBinarySearch(initialKey);
        } else {
            nextValue = reader.getFirstEntry();
        }
        this.to = to;
    }

    @Override
    public boolean hasNext() {
        return nextValue != null && (to == null || to.compareTo(nextValue.key()) > 0);
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        if(!hasNext()) {
            return null;
        }
        BaseEntry<ByteBuffer> temp = nextValue;
        nextValue = reader.getNextEntry();
        return temp;
    }

    public BaseEntry<ByteBuffer> peek() {
        if(!hasNext()) {
            return null;
        }
        return nextValue;
    }
}
