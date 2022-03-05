package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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

    public InMemoryDao(Config config) {
        this.pathToFile = config.basePath().resolve("file1.txt").toString();
        try {
            if (!Files.exists(Path.of(this.pathToFile))) {
                Files.createFile(Path.of(this.pathToFile));
            }
            read();
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
    public void upsert(BaseEntry<ByteBuffer> entry) {
        data.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        close();
        read();
    }

    @Override
    public void close() throws IOException {
        write();
    }

    private void write() throws IOException {
        try (FileOutputStream fout = new FileOutputStream(pathToFile)) {
            writeInt(fout, data.size());
            for (Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>> entry: data.entrySet()) {
                // Write 2 times due to entry.key = entry.value.key
                writeByteBuffer(fout, entry.getKey());
                writeByteBuffer(fout, entry.getValue().value());
            }
        }
    }

    private void writeInt(FileOutputStream fout, int a) throws IOException {
        fout.write(ByteBuffer.allocate(Integer.BYTES).putInt(a).array());
    }

    private void writeByteBuffer(FileOutputStream fout, ByteBuffer bb) throws IOException {
        writeInt(fout, bb.remaining());
        fout.write(bb.array());
    }

    private void read() throws IOException {
        try (FileInputStream fin = new FileInputStream(pathToFile)) {
            if (fin.available() == 0) {
                return;
            }

            ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> newData = new ConcurrentSkipListMap<>();
            int sizeOfData = readInt(fin);
            while (fin.available() != 0) {
                for (int i = 0; i < sizeOfData; ++i) {
                    // Read 2 times due to entry.key = entry.value.key
                    ByteBuffer key = readByteBuffer(fin);
                    ByteBuffer value = readByteBuffer(fin);
                    newData.put(key, new BaseEntry<>(key, value));
                }
            }
            this.data = newData;
        }
    }

    private int readInt(FileInputStream fin) throws IOException {
        byte[] bytes = new byte[Integer.BYTES];
        fin.read(bytes);
        return ByteBuffer.allocate(bytes.length).put(bytes).flip().getInt();
    }

    private ByteBuffer readByteBuffer(FileInputStream fin) throws IOException {
        int numberOfBytes = readInt(fin);
        byte[] bytes = new byte[numberOfBytes];
        fin.read(bytes);
        return ByteBuffer.wrap(bytes);
    }

}
