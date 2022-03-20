package ru.mail.polis.andreyilchenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistentDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private static final String DATA_FILE = "data.txt";
    private static final String OFFSETS_FILE = "offsets.txt";
    private static final int DEFAULT_ALLOCATE_BUFFER_WRITE_SIZE = 0xA00;

    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> entries = new ConcurrentSkipListMap<>();
    private final Path pathToData;
    private final Path pathToOffsets;
    private final boolean dataExistFlag;
    private final int allocateBufferWriteSize;

    public PersistentDao(Config config) {
        this(config, DEFAULT_ALLOCATE_BUFFER_WRITE_SIZE);
    }

    public PersistentDao(Config config, int allocateBufferWriteSize) {
        this.allocateBufferWriteSize = allocateBufferWriteSize;
        pathToData = config.basePath().resolve(DATA_FILE);
        pathToOffsets = config.basePath().resolve(OFFSETS_FILE);
        dataExistFlag = Files.exists(pathToData) && Files.exists(pathToOffsets);
    }

    @Override
    public void flush() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(2 * Integer.BYTES * entries.size() + Integer.BYTES);
        int pos = 0;
        for (BaseEntry<ByteBuffer> entry : entries.values()) {
            buf.putInt(pos);
            pos += entry.key().remaining();
            buf.putInt(pos);
            pos += entry.value().remaining();
        }
        buf.putInt(pos);
        buf.flip();
        try (
                RandomAccessFile dataAccessFile = new RandomAccessFile(pathToData.toFile(), "rw");
                FileChannel dataChannel = dataAccessFile.getChannel();
                FileOutputStream offsetsOutputStream = new FileOutputStream(pathToOffsets.toFile());
                FileChannel offsetsChannel = offsetsOutputStream.getChannel()
        ) {
            offsetsChannel.write(buf);
            ByteBuffer bufferToWrite = ByteBuffer.allocate(allocateBufferWriteSize);
            for (BaseEntry<ByteBuffer> entry : entries.values()) {
                int keyLen = entry.key().remaining();
                int valueLen = entry.value().remaining();
                if (bufferToWrite.remaining() + keyLen + valueLen >= allocateBufferWriteSize) {
                    dataChannel.write(bufferToWrite.flip());
                    bufferToWrite.clear();
                }
                bufferToWrite.put(entry.key()).put(entry.value());
            }
            dataChannel.write(bufferToWrite.flip());
        }
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> entry = entries.get(key);
        if (entry == null && dataExistFlag) {
            return findInFile(key);
        }
        return entry;
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        entries.put(entry.key(), entry);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (to == null && from == null) {
            return entries.values().iterator();
        }
        if (to == null) {
            return entries.tailMap(from).values().iterator();
        }
        if (from == null) {
            return entries.headMap(to).values().iterator();
        }
        return entries.subMap(from, to).values().iterator();
    }

    private BaseEntry<ByteBuffer> findInFile(ByteBuffer key) throws IOException {
        try (
                RandomAccessFile dataReader = new RandomAccessFile(pathToData.toFile(), "r");
                FileChannel dataChannel = dataReader.getChannel();
                RandomAccessFile offsetsReader = new RandomAccessFile(pathToOffsets.toFile(), "r")
        ) {
            long startIndex = 0;
            long endIndex = (offsetsReader.length() - 4) / 8;
            long midIndex;
            while (startIndex <= endIndex) {
                midIndex = ((endIndex + startIndex) / 2);
                offsetsReader.seek(midIndex * 8);
                int keyStartOffset = offsetsReader.readInt();
                int valueStartOffset = offsetsReader.readInt();
                int valueEndOffset = offsetsReader.readInt();
                ByteBuffer probableKey = ByteBuffer.allocate(valueStartOffset - keyStartOffset);
                dataChannel.read(probableKey, keyStartOffset);
                probableKey.flip();
                int compareResult = probableKey.compareTo(key);
                if (compareResult == 0) {
                    ByteBuffer value = ByteBuffer.allocate(valueEndOffset - valueStartOffset);
                    dataChannel.read(value, valueStartOffset);
                    value.flip();
                    return new BaseEntry<>(key, value);
                } else if (compareResult > 0) {
                    endIndex = midIndex - 1;
                } else {
                    startIndex = midIndex + 1;
                }
            }
        }
        return null;
    }
}
