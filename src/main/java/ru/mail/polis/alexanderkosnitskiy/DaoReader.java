package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoReader implements Closeable {
    private final FileInputStream reader;

    public DaoReader(String name) throws FileNotFoundException {
        reader = new FileInputStream(name);
    }

    public BaseEntry<ByteBuffer> retrieveElement(ByteBuffer key) throws IOException {
        int size = readInt();
        BaseEntry<ByteBuffer> elem;
        for (int i = 0; i < size; i++) {
            elem = readElementPair();
            if(elem.key().compareTo(key) == 0) {
                return elem;
            }
        }
        return null;
    }

    public int checkForKey(ByteBuffer key) throws IOException {
        int size = readInt();
        ByteBuffer prevElem = null, elem;
        for (int i = 0; i < size; i++) {
            elem = readElementPair().key();

            if(i == 0 && elem.compareTo(key) > 0) {
                return -1;
            }
            else if(prevElem != null && prevElem.compareTo(key) < 0 && elem.compareTo(key) > 0) {
                return 404;
            }
            else if(elem.compareTo(key) < 0 && i + 1 == size) {
                return 1;
            }
            else if(elem.compareTo(key) == 0) {
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
}
