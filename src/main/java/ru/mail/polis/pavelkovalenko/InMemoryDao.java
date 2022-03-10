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
    private final Path pathToDataFile;

    public InMemoryDao(Config config) throws IOException {
        pathToDataFile = config.basePath().resolve("data.txt");
        if (!Files.exists(pathToDataFile)) {
            Files.createDirectories(config.basePath());
            Files.createFile(pathToDataFile);
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
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> result = findKeyInMap(key);
        return result == null ? findKeyInFile(key) : result;
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

    private BaseEntry<ByteBuffer> findKeyInMap(ByteBuffer key) {
        return data.get(key);
    }

    private BaseEntry<ByteBuffer> findKeyInFile(ByteBuffer key) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(pathToDataFile.toString(), "r")) {
            return binarySearchInFile(raf, key);
        }
    }

    private boolean reachedEOF(RandomAccessFile raf) throws IOException {
        return raf.getFilePointer() <= 0 || raf.getFilePointer() == raf.length();
    }

    private BaseEntry<ByteBuffer> binarySearchInFile(RandomAccessFile raf, ByteBuffer key) throws IOException {
        long a = 0;
        long b = raf.length();
        while (b - a > Utils.MIN_DISTANCE_BETWEEN_PAIRS) {
            long c = (b + a) / 2;
            raf.seek(c);
            rollbackToPairStart(raf);

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

        return null;
    }

    private void rollbackToPairStart(RandomAccessFile raf) throws IOException {
        long curPos = raf.getFilePointer();
        if (curPos % 2 == 1) {
            --curPos;
        }
        while (!reachedEOF(raf) && !isPairSeparator(raf.readChar())) {
            curPos -= Character.BYTES;
            raf.seek(Math.max(curPos, 0));
        }
    }

    private boolean isPairSeparator(char ch) {
        return ch == Utils.PAIR_SEPARATOR;
    }

    private ByteBuffer readByteBuffer(RandomAccessFile raf) throws IOException {
        int bbSize = raf.readInt();
        ByteBuffer bb = ByteBuffer.allocate(bbSize);
        raf.getChannel().read(bb);
        bb.rewind();
        return bb;
    }

    private void write() throws IOException {
        if (data.isEmpty()) {
            return;
        }

        try (RandomAccessFile raf = new RandomAccessFile(pathToDataFile.toString(), "rw")) {
            for (BaseEntry<ByteBuffer> entry: data.values()) {
                int bbSize = Integer.BYTES + entry.key().remaining()
                           + Integer.BYTES + entry.value().remaining() + Character.BYTES;
                ByteBuffer bb = ByteBuffer.allocate(bbSize);

                bb.putInt(entry.key().remaining());
                bb.put(entry.key());
                bb.putInt(entry.value().remaining());
                bb.put(entry.value());
                bb.putChar(Utils.PAIR_SEPARATOR);
                bb.rewind();

                raf.getChannel().write(bb);
            }
        }
    }

}
