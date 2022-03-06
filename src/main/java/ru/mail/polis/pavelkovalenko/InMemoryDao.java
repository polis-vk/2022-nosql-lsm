package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> data = new ConcurrentSkipListMap<>();
    private final List<String> dataFiles = new ArrayList<>();
    private static final int DATA_SIZE_TRESHOLD = 20_000;
    private volatile boolean dataWasChanged;
    private final ReentrantLock lock = new ReentrantLock();
    private final Config config;

    public InMemoryDao(Config config) {
        this.config = config;
        try {
            File configDir = new File(config.basePath().toString());
            String[] filenames = configDir.list();

            if (filenames == null || filenames.length == 0) {
                addDataFile("data0.txt");
                return;
            }

            for (String filename: filenames) {
                addDataFile(filename);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        return null;
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) {
        try {
            flushIfChanged();

            BaseEntry<ByteBuffer> result = null;
            for (String dataFile: dataFiles) {
                result = findEntry(dataFile, key);

                if (result.key().equals(key)) {
                    break;
                }
            }
            return result;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        try {
            lock.lock();
            if (data.size() >= DATA_SIZE_TRESHOLD) {
                flush();
                addDataFile("data" + dataFiles.size() + ".txt");
            }
            lock.unlock();

            data.put(entry.key(), entry);
            dataWasChanged = true;
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

    private void addDataFile(String filename) throws IOException {
        String dataFile = config.basePath().resolve(filename).toString();
        dataFiles.add(dataFile);
        if (!Files.exists(Path.of(dataFile))) {
            Files.createFile(Path.of(dataFile));
        }
    }

    private BaseEntry<ByteBuffer> findEntry(String dataFile, ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> result;
        try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw")) {
            do {
                result = readEntry(raf);
            } while (!result.key().equals(key) && !reachedEOF(raf));
        }
        return result;
    }

    private boolean reachedEOF(RandomAccessFile raf) throws IOException {
        return raf.getFilePointer() == raf.length();
    }

    private void flushIfChanged() throws IOException {
        if (dataWasChanged) {
            flush();
            dataWasChanged = false;
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
        if (data.isEmpty()) {
            return;
        }

        try (RandomAccessFile raf = new RandomAccessFile(dataFiles.get(dataFiles.size() - 1), "rw")) {
            raf.setLength(0);
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

}
