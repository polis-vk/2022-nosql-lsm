package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoReader implements Closeable {
    private final RandomAccessFile reader;
    private final long size;

    public DaoReader(String name) throws IOException {
        reader = new RandomAccessFile(name, "r");
        size = reader.length();
    }

    public BaseEntry<ByteBuffer> retrieveElement(ByteBuffer key) throws IOException {
        int size = readInt();
        BaseEntry<ByteBuffer> elem;
        for (int i = 0; i < size; i++) {
            elem = readElementPair();
            if (elem.key().compareTo(key) == 0) {
                return elem;
            }
        }
        return null;
    }

    public BaseEntry<ByteBuffer> binarySearch(ByteBuffer key) throws IOException {
        readInt();
        long lowerBond = 0;
        long higherBond = this.size;
        long middle = higherBond / 2;

        while (lowerBond <= higherBond) {
            FileEntry result = nextEntry(middle);
            if (key.compareTo(result.entry.key()) > 0) {
                lowerBond = middle + 1;
            } else if (key.compareTo(result.entry.key()) < 0) {
                higherBond = middle - 1;
            } else if (key.compareTo(result.entry.key()) == 0) {
                return result.entry;
            }
            middle = (lowerBond + higherBond) / 2;
        }
        return null;
    }

    private FileEntry nextEntry(long startingPosition) throws IOException {
        int bufSize = 128;
        FileEntry entry = new FileEntry();
        ByteBuffer buffer = ByteBuffer.allocate(bufSize);
        reader.seek(startingPosition);
        byte[] bytes;
        byte result = 'n';
        long position = startingPosition;
        while (position != size) {
            reader.read(buffer.array());
            bytes = buffer.array();
            for (int i = 0; i < bytes.length; i++) {
                if (bytes[i] == '\n') {
                    position += i;
                    entry.bytePosition = position;
                    result = '\n';
                    break;
                }
            }
            if (result == '\n') {
                break;
            }
            position += bufSize;
        }
        reader.seek(position - Integer.BYTES);
        entry.size = readInt();

        reader.seek(position - entry.size);

        entry.entry = readElementPair();

        return entry;
    }

    public int checkForKey(ByteBuffer key) throws IOException {
        int size = readInt();
        ByteBuffer prevElem = null;
        ByteBuffer elem;
        for (int i = 0; i < size; i++) {
            elem = readElementPair().key();

            if (i == 0 && elem.compareTo(key) > 0) {
                return -1;
            } else if (prevElem != null && prevElem.compareTo(key) < 0 && elem.compareTo(key) > 0) {
                return 404;
            } else if (elem.compareTo(key) < 0 && i + 1 == size) {
                return 1;
            } else if (elem.compareTo(key) == 0) {
                return 0;
            }

            prevElem = elem;
        }
        return -2;
    }

    public ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> readMap() throws IOException {
        int size = readInt();
        ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map = new ConcurrentSkipListMap<>();
        BaseEntry<ByteBuffer> entry;
        for (int i = 0; i < size; i++) {
            entry = readElementPair();
            map.put(entry.key(), entry);
        }
        return map;
    }

    public BaseEntry<ByteBuffer> readElementPair() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * 2);
        reader.read(buffer.array());
        buffer.rewind();
        int keyLen = buffer.getInt();
        int valLen = buffer.getInt();
        ByteBuffer keyBuffer = ByteBuffer.allocate(keyLen);
        ByteBuffer valBuffer = ByteBuffer.allocate(valLen);
        reader.read(keyBuffer.array());
        reader.read(valBuffer.array());
        keyBuffer.rewind();
        valBuffer.rewind();
        reader.read(buffer.array(), 0, Integer.BYTES + Byte.BYTES);
        return new BaseEntry<ByteBuffer>(keyBuffer, valBuffer);
    }

    public ByteBuffer readElement() throws IOException {
        int size = readInt();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        reader.read(buffer.array());
        buffer.rewind();
        return buffer;
    }

    public int readInt() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        reader.read(buffer.array());
        buffer.rewind();
        return buffer.getInt();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private static class FileEntry {
        public int place;
        public long bytePosition;
        public int size;
        BaseEntry<ByteBuffer> entry;
    }
}
