package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> data = new ConcurrentSkipListMap<>();
    private final String pathToFile;
    private final int DATA_SIZE_TRESHOLD = 25_000;

    public InMemoryDao(Config config) {
        this.pathToFile = config.basePath().resolve("DB1.txt").toString();
        try {
            if (!Files.exists(Path.of(this.pathToFile))) {
                Files.createFile(Path.of(this.pathToFile));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        try {
            flush();
            return new DBIterator(from, to);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        try {
            if (data.size() > DATA_SIZE_TRESHOLD) {
                flush();
            }
            data.put(entry.key(), entry);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush() throws IOException {
        write();
        data.clear();
    }

    @Override
    public void close() throws IOException {
        write();
    }

    private void read() throws IOException {
        data.clear();
        DBIterator dbIterator = new DBIterator(null, null);
        while (dbIterator.hasNext()) {
            BaseEntry<ByteBuffer> entry = dbIterator.next();
            upsert(entry);
        }
    }

    private BaseEntry<ByteBuffer> readEntry(RandomAccessFile raf) throws IOException {
        ByteBuffer key = readByteBuffer(raf);
        ByteBuffer value = readByteBuffer(raf);
        raf.readLine();
        return new BaseEntry<>(key, value);
    }

    private ByteBuffer readByteBuffer(RandomAccessFile raf) throws IOException {
        int numberOfBytes = raf.readInt();
        byte[] bytes = new byte[numberOfBytes];
        raf.read(bytes);
        return ByteBuffer.wrap(bytes);
    }

    private void write() throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(pathToFile, "w")) {
            for (Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>> entry: data.entrySet()) {
                writeEntry(raf, entry.getValue());
            }
        }
    }

    private void writeEntry(RandomAccessFile raf, BaseEntry<ByteBuffer> entry) throws IOException {
        writeByteBuffer(raf, entry.key());
        writeByteBuffer(raf, entry.value());
        raf.write('\n');
    }

    private void writeByteBuffer(RandomAccessFile raf, ByteBuffer bb) throws IOException {
        raf.writeInt(bb.remaining());
        raf.write(bb.array());
    }

    private class DBIterator implements Iterator<BaseEntry<ByteBuffer>> {

        private final RandomAccessFile raf;
        private final ByteBuffer to;

        public DBIterator(ByteBuffer from, ByteBuffer to) throws IOException {
            raf = new RandomAccessFile(pathToFile, "r");
            this.to = to;
            boolean foundFrom = false;

            if (from != null) {
                while (hasNext()) {
                    ByteBuffer read = next().key();
                    if (read.equals(from)) {
                        foundFrom = true;
                        break;
                    }
                }

                if (!foundFrom) {
                    throw new IllegalArgumentException("No such key in data");
                }
            }
        }

        @Override
        public boolean hasNext() {
            return raf.getChannel().isOpen();
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            try {
                BaseEntry<ByteBuffer> readEntry = readEntry(raf);
                if (to == null && !hasNext() || readEntry.key().equals(to)) {
                    raf.close();
                }
                return readEntry;
            } catch (IOException e) {
                return null;
            }
        }
    }

}
