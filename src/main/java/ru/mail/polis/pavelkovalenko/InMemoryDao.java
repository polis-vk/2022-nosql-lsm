package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> data = new ConcurrentSkipListMap<>();
    private final String pathToDataFile;

    public InMemoryDao(Config config) {
        pathToDataFile = config.basePath().resolve("data.txt").toString();
        try {
            if (!Files.exists(Path.of(pathToDataFile))) {
                Files.createFile(Path.of(pathToDataFile));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from != null && to != null) {
            return data.subMap(from, to).values().iterator();
        }
        if (from != null) {
            return data.tailMap(from).values().iterator();
        }
        if (to != null) {
            return data.headMap(to).values().iterator();
        }
        return data.values().iterator();
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) {
        try {
            return findKey(key);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        data.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        write();
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private BaseEntry<ByteBuffer> findKey(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> result = findKeyInMap(key);
        return result == null ? findKeyInFile(key) : result;
    }

    private BaseEntry<ByteBuffer> findKeyInMap(ByteBuffer key) {
        return data.get(key);
    }

    private BaseEntry<ByteBuffer> findKeyInFile(ByteBuffer key) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(pathToDataFile, "r")) {
            return binarySearchInFile(raf, key);
        }
    }

    private BaseEntry<ByteBuffer> binarySearchInFile(RandomAccessFile raf, ByteBuffer key) throws IOException {
        long a = 0;
        long b = raf.length();
        while (b - a > 20) {
            long c = (b + a) / 2;
            raf.seek(c);
            raf.readLine();
            ByteBuffer curKey = readByteBuffer(raf);
            int compare = curKey.compareTo(key);
            if (compare < 0) {
                a = c;
            } else if (compare == 0) {
                return new BaseEntry<>(curKey, readByteBuffer(raf));
            } else {
                b = c;
            }
        }

        raf.seek(a);
        return new BaseEntry<>(readByteBuffer(raf), readByteBuffer(raf));
    }

    private ByteBuffer readByteBuffer(RandomAccessFile raf) throws IOException {
        int bbSize = raf.readInt();
        byte[] bytes = new byte[bbSize];
        raf.read(bytes);
        return ByteBuffer.wrap(bytes);
    }

    private void write() throws IOException {
        if (data.isEmpty()) {
            return;
        }

        ByteBuffer bb;
        int bbSize = (Integer.BYTES + 11 + Integer.BYTES + 11 + Character.BYTES) * data.size();
        try (RandomAccessFile raf = new RandomAccessFile(pathToDataFile, "rw")) {
            bb = ByteBuffer.wrap(new byte[bbSize]);
            for (BaseEntry<ByteBuffer> entry: data.values()) {
                bb.putInt(entry.key().remaining());
                bb.put(entry.key());
                bb.putInt(entry.value().remaining());
                bb.put(entry.value());
                bb.putChar('\n');
            }
            raf.write(bb.array());
        }
    }

}
