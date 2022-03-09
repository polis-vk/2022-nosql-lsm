package ru.mail.polis.test.arturgaleev;

import ru.mail.polis.BaseEntry;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class FileDBReader implements Closeable {

    private int size;
    private int[] positions;
    private final FileChannel channel;
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);

    public FileDBReader(String name) throws IOException {
        RandomAccessFile reader = new RandomAccessFile(name, "r");
        channel = reader.getChannel();
    }

    public void readArrayLinks() throws IOException {
        size = readInt();
        int mapBeginPos = (size + 1) * Integer.BYTES;
        positions = new int[size];
        for (int i = 0; i < size; i++) {
            positions[i] = mapBeginPos + readInt();
        }
    }

    protected int readInt() throws IOException {
        buffer.clear();
        channel.read(buffer);
        buffer.rewind();
        return buffer.getInt();
    }

    protected BaseEntry<ByteBuffer> readEntry() throws IOException {
        int keyLength = readInt();
        int valueLength = readInt();
        ByteBuffer keyBuffer = ByteBuffer.allocate(keyLength);
        channel.read(keyBuffer);
        ByteBuffer valueBuffer = ByteBuffer.allocate(valueLength);
        channel.read(valueBuffer);
        keyBuffer.rewind();
        valueBuffer.rewind();
        return new BaseEntry<>(keyBuffer, valueBuffer);
    }

    public ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> readMap() throws IOException {
        ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map = new ConcurrentSkipListMap<>();
        channel.position(positions[0]);
        BaseEntry<ByteBuffer> entry;
        for (int i = 0; i < size; i++) {
            entry = readEntry();
            map.put(entry.key(), entry);
        }
        return map;
    }

    public BaseEntry<ByteBuffer> getByPos(int pos) throws IOException {
        channel.position(positions[pos]);
        return readEntry();
    }

    public BaseEntry<ByteBuffer> getByKey(ByteBuffer key) throws IOException {
        int low = 0;
        int high = size - 1;
        while (low <= high) {
            int mid = low + ((high - low) / 2);
            int result = getByPos(mid).key().compareTo(key);
            if (result < 0) {
                low = mid + 1;
            } else if (result > 0) {
                high = mid - 1;
            } else {
                return getByPos(mid);
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    public String toString(ByteBuffer data) {
        return data == null ? null : new String(data.array(),
                data.arrayOffset() + data.position(), data.remaining(), StandardCharsets.UTF_8);
    }
}
