package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> data = new ConcurrentSkipListMap<>();
    private final String pathToDataFile;
    private static final int DATA_SIZE_TRESHOLD = 20_000;
    private long lastWrittenPos;
    private boolean dataWasChanged;

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
            return findEntry(key);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        try {


            data.put(entry.key(), entry);
            dataWasChanged = true;
            if (data.size() >= DATA_SIZE_TRESHOLD) {
                flush();
                data.clear();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush() throws IOException {
        if (dataWasChanged) {
            write();
            dataWasChanged = false;
        }
    }

    @Override
    public void close() throws IOException {
        write();
    }

    private BaseEntry<ByteBuffer> findEntry(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> result = findEntryInMap(key);
        return result == null ? findEntryInFiles(key) : result;
    }

    private BaseEntry<ByteBuffer> findEntryInMap(ByteBuffer key) {
        return data.get(key);
    }

    private BaseEntry<ByteBuffer> findEntryInFiles(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> result;
        try (RandomAccessFile raf = new RandomAccessFile(pathToDataFile, "r")) {
            do {
                result = readEntry(raf);
            } while (!result.key().equals(key) && !reachedEOF(raf));

            if (reachedEOF(raf) && !result.key().equals(key)) {
                return null;
            }

            data.clear();
            while (!reachedEOF(raf) && data.size() < DATA_SIZE_TRESHOLD) {
                BaseEntry<ByteBuffer> entry = readEntry(raf);
                data.put(entry.key(), entry);
            }
        }
        return result;
    }

    private boolean reachedEOF(RandomAccessFile raf) throws IOException {
        return raf.getFilePointer() == raf.length();
    }

    private BaseEntry<ByteBuffer> readEntry(RandomAccessFile raf) throws IOException {
        ByteBuffer key = readByteBuffer(raf);
        ByteBuffer value = readByteBuffer(raf);
        return new BaseEntry<>(key, value);
    }

    private ByteBuffer readByteBuffer(RandomAccessFile raf) throws IOException {
        String str = raf.readLine();
        return ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8));
    }

    private void write() throws IOException {
        if (data.isEmpty()) {
            return;
        }

        try (RandomAccessFile raf = new RandomAccessFile(pathToDataFile, "rw")) {
            raf.seek(lastWrittenPos);
            for (BaseEntry<ByteBuffer> entry: data.values()) {
                writeEntry(raf, entry);
            }
            lastWrittenPos = raf.getFilePointer();
        }
    }

    private void writeEntry(RandomAccessFile raf, BaseEntry<ByteBuffer> entry) throws IOException {
        writeByteBuffer(raf, entry.key());
        raf.write('\n');
        writeByteBuffer(raf, entry.value());
        raf.write('\n');
    }

    private void writeByteBuffer(RandomAccessFile raf, ByteBuffer bb) throws IOException {
        raf.getChannel().write(bb);
    }

}
