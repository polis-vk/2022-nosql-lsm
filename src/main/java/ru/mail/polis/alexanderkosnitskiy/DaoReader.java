package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoReader implements Closeable {
    private final RandomAccessFile reader;
    private final RandomAccessFile indexReader;

    public DaoReader(Path fileName, Path indexName) throws IOException {
        reader = new RandomAccessFile(String.valueOf(fileName), "r");
        indexReader = new RandomAccessFile(String.valueOf(indexName), "r");
    }

    public BaseEntry<ByteBuffer> binarySearch(ByteBuffer key) throws IOException {
        long lowerBond = 0;
        long higherBond = readSize();
        long middle = higherBond / 2;

        while (lowerBond <= higherBond) {
            BaseEntry<ByteBuffer> result = getEntry(middle);
            if (key.compareTo(result.key()) > 0) {
                lowerBond = middle + 1;
            } else if (key.compareTo(result.key()) < 0) {
                higherBond = middle - 1;
            } else if (key.compareTo(result.key()) == 0) {
                return result;
            }
            middle = (lowerBond + higherBond) / 2;
        }
        return null;
    }

    private BaseEntry<ByteBuffer> getEntry(long middle) throws IOException {
        indexReader.seek(Integer.BYTES + middle * Long.BYTES);
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        indexReader.getChannel().read(buffer);
        buffer.rewind();
        reader.seek(buffer.getLong());
        return readElementPair();
    }

    public BaseEntry<ByteBuffer> readElementPair() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * 2);
        reader.read(buffer.array());
        buffer.rewind();
        int keyLen = buffer.getInt();
        int valLen = buffer.getInt();
        ByteBuffer keyBuffer = ByteBuffer.allocate(keyLen);
        ByteBuffer valBuffer = ByteBuffer.allocate(valLen);
        reader.getChannel().read(keyBuffer);
        reader.getChannel().read(valBuffer);
        keyBuffer.rewind();
        valBuffer.rewind();
        return new BaseEntry<>(keyBuffer, valBuffer);
    }

    public int readSize() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        indexReader.getChannel().read(buffer);
        buffer.rewind();
        return buffer.getInt();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    //Метод для считывания всей мапы - вдруг понадобится
    public ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> readMap() throws IOException {
        int mapSize = readSize();
        ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map = new ConcurrentSkipListMap<>();
        BaseEntry<ByteBuffer> entry;
        for (int i = 0; i < mapSize; i++) {
            entry = readElementPair();
            map.put(entry.key(), entry);
        }
        return map;
    }

    //Метод для итеративного поиска - не используется
    //Оставил, так как он всегда железно работает и хорош для тестов других, более быстрых
    //Но опасных методов
    public BaseEntry<ByteBuffer> retrieveElement(ByteBuffer key) throws IOException {
        int mapSize = readSize();
        BaseEntry<ByteBuffer> elem;
        for (int i = 0; i < mapSize; i++) {
            elem = readElementPair();
            if (elem.key().compareTo(key) == 0) {
                return elem;
            }
        }
        return null;
    }
}
