package ru.mail.polis.levsaskov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private static final int ALLOC_SIZE = 2048;
    private static final int BYTES_IN_INT = 4;
    private static final String MEM_FILENAME = "daoMem.bin";

    private Path fileConfigPath;
    private final ConcurrentSkipListMap<ByteBuffer, BaseEntry<ByteBuffer>> entrys = new ConcurrentSkipListMap<>();

    public InMemoryDao() {
    }

    public InMemoryDao(Config config) {
        this.fileConfigPath = config.basePath().resolve(MEM_FILENAME);
        if (!fileConfigPath.toFile().exists()) {
            return;
        }

        try (
                BufferedInputStream bs = new BufferedInputStream(new FileInputStream(fileConfigPath.toFile()), ALLOC_SIZE);
                DataInputStream stream = new DataInputStream(bs)
        ) {
            while (stream.available() >= BYTES_IN_INT) {
                upsert(readEntry(stream));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        Iterator<BaseEntry<ByteBuffer>> ans;

        if (from == null && to == null) {
            ans = entrys.values().iterator();
        } else if (from == null) {
            ans = entrys.headMap(to).values().iterator();
        } else if (to == null) {
            ans = entrys.tailMap(from).values().iterator();
        } else {
            ans = entrys.subMap(from, to).values().iterator();
        }

        return ans;
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        entrys.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        try (
                RandomAccessFile daoMemfile = new RandomAccessFile(fileConfigPath.toFile(), "rw");
                FileChannel channel = daoMemfile.getChannel();
        ) {
            ByteBuffer bufferToWrite = ByteBuffer.allocate(ALLOC_SIZE);
            for (BaseEntry<ByteBuffer> entry : entrys.values()) {
                persistEntry(entry, bufferToWrite);
                channel.write(bufferToWrite);
                bufferToWrite.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void persistEntry(BaseEntry<ByteBuffer> entry, ByteBuffer bufferToWrite) {
        bufferToWrite.putInt(entry.key().array().length);
        bufferToWrite.put(entry.key().array());

        bufferToWrite.putInt(entry.value().array().length);
        bufferToWrite.put(entry.value().array());
        bufferToWrite.flip();
    }

    private static BaseEntry<ByteBuffer> readEntry(DataInputStream stream) throws IOException {
        int keyLen = stream.readInt();
        byte[] key = new byte[keyLen];
        stream.read(key);

        int valueLen = stream.readInt();
        byte[] value = new byte[valueLen];
        stream.read(value);

        return new BaseEntry<>(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
    }
}
