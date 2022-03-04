package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.test.pavelkovalenko.ByteBufferDaoFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>>, Serializable {

    ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> data = new ConcurrentSkipListMap<>();
    Config config;
    String pathToFile;

    public InMemoryDao(Config config) {
        this.config = config;
        this.pathToFile = new StringBuilder(config.basePath().toString()).append("\\file1.txt").toString();
        try {
            if (!Files.exists(Path.of(this.pathToFile))) {
                Files.createFile(Path.of(this.pathToFile));
            }
            read();
            if (!this.data.isEmpty()) {
                System.out.println("DATA: key=" + new ByteBufferDaoFactory().toString(this.data.firstKey())
                        + ", value=" + new ByteBufferDaoFactory().toString(this.data.values().iterator().next().value()));
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
        return Dao.super.get(key);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> allFrom(ByteBuffer from) {
        return Dao.super.allFrom(from);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> allTo(ByteBuffer to) {
        return Dao.super.allTo(to);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> all() {
        return Dao.super.all();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        data.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        close();
        reopen();
    }

    @Override
    public void close() {
        try {
            write();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public InMemoryDao reopen() {
        try {
            read();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return this;
    }

    public Config getConfig() {
        return config;
    }

    private void write() throws IOException {
        try (FileOutputStream fout = new FileOutputStream(pathToFile)) {
            writeInt(fout, data.size());
            for (Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>> entry: data.entrySet()) {
                // ���������� 2 ����, ��� ��� entry.key = entry.value.key
                writeByteBuffer(fout, entry.getKey());
                writeByteBuffer(fout, entry.getValue().value());
            }
        }
    }

    private void writeInt(FileOutputStream fout, int a) throws IOException {
        fout.write(ByteBuffer.allocate(Integer.BYTES).putInt(a).array());
    }

    private void writeByteBuffer(FileOutputStream fout, ByteBuffer bb) throws IOException {
        writeInt(fout, bb.array().length);
        fout.write(bb.array());
    }

    private InMemoryDao read() throws IOException {
        try (FileInputStream fin = new FileInputStream(pathToFile)) {
            if (fin.available() == 0) {
                return null;
            }

            ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> newData = new ConcurrentSkipListMap<>();
            int sizeOfData = readInt(fin);
            while (fin.available() != 0) {
                for (int i = 0; i < sizeOfData; ++i) {
                    // ������ 2 ����, ��� ��� entry.key = entry.value.key
                    ByteBuffer key = readByteBuffer(fin);
                    ByteBuffer value = readByteBuffer(fin);
                    newData.put(key, new BaseEntry<>(key, value));
                }
            }
            this.data = newData;
            return this;
        }
    }

    private int readInt(FileInputStream fin) throws IOException {
        byte[] bytes = new byte[Integer.BYTES];
        fin.read(bytes);
        return ByteBuffer.allocate(bytes.length).put(bytes).flip().getInt();
    }

    private ByteBuffer readByteBuffer(FileInputStream fin) throws IOException {
        int nBytes = readInt(fin);
        byte[] bytes = new byte[nBytes];
        fin.read(bytes);
        return ByteBuffer.wrap(bytes);
    }

}
