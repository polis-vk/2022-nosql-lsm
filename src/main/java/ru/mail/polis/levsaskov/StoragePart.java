package ru.mail.polis.levsaskov;

import org.jetbrains.annotations.Nullable;
import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class StoragePart implements AutoCloseable {
    public static final int LEN_FOR_NULL = -1;

    private int storagePartN;
    private MappedByteBuffer memoryBB;
    private MappedByteBuffer indexBB;
    private int entrysC;

    public void init(Path memoryPath, Path indexPath, int storagePartN) throws IOException {
        this.storagePartN = storagePartN;
        memoryBB = mapFile(memoryPath);
        indexBB = mapFile(indexPath);
        entrysC = indexBB.capacity() / Long.BYTES;
    }

    public BaseEntry<ByteBuffer> get(ByteBuffer key) {
        int position = binarySearch(entrysC - 1, key);
        BaseEntry<ByteBuffer> res = readEntry(position);
        return res.key().equals(key) ? res : null;
    }

    public PeekIterator get(ByteBuffer from, ByteBuffer to) {
        return new PeekIterator(new StoragePartIterator(from, to), storagePartN);
    }

    @Override
    public void close() {
        unmap(indexBB);
        indexBB = null;
        unmap(memoryBB);
        memoryBB = null;
    }

    private int binarySearch(int inLast, ByteBuffer key) {
        if (key == null) {
            return 0;
        }

        int first = 0;
        int last = inLast;
        int position = (first + last) / 2;
        BaseEntry<ByteBuffer> curEntry = readEntry(position);

        while (!curEntry.key().equals(key) && first <= last) {
            if (curEntry.key().compareTo(key) > 0) {
                last = position - 1;
            } else {
                first = position + 1;
            }
            position = (first + last) / 2;
            curEntry = readEntry(position);
        }
        return position;
    }

    private BaseEntry<ByteBuffer> readEntry(int entryN) {
        int ind = (int) indexBB.getLong(entryN * Long.BYTES);
        byte[] key = readBytes(ind);
        assert (key != null);
        ind += Integer.BYTES + key.length;
        byte[] value = readBytes(ind);
        return new BaseEntry<>(ByteBuffer.wrap(key), value == null ? null : ByteBuffer.wrap(value));
    }

    private byte[] readBytes(int ind) {
        int currInd = ind;
        int len = memoryBB.getInt(currInd);
        if (len == LEN_FOR_NULL) {
            return null;
        }
        currInd += Integer.BYTES;
        byte[] bytes = new byte[len];
        memoryBB.get(currInd, bytes);
        return bytes;
    }

    private static MappedByteBuffer mapFile(Path filePath) throws IOException {

        MappedByteBuffer mappedFile;
        try (
                FileChannel fileChannel = (FileChannel) Files.newByteChannel(filePath,
                        EnumSet.of(StandardOpenOption.READ))
        ) {
            mappedFile = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        }

        return mappedFile;
    }

    private static void unmap(MappedByteBuffer buffer) {
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Object unsafe = unsafeField.get(null);
            Method invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
            invokeCleaner.invoke(unsafe, buffer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private class StoragePartIterator implements Iterator<BaseEntry<ByteBuffer>> {
        private int nextPos;
        private final ByteBuffer to;
        private BaseEntry<ByteBuffer> next;

        public StoragePartIterator(ByteBuffer from, ByteBuffer to) {
            this.to = to;
            nextPos = binarySearch(entrysC - 1, from);
            // Граничные случаи
            if (nextPos + 1 < entrysC && from != null
                    && readEntry(nextPos).key().compareTo(from) < 0) {
                nextPos++;
            }

            next = readEntry(nextPos);

            if (from != null && next.key().compareTo(from) < 0) {
                next = null;
            }
        }

        @Override
        public boolean hasNext() {
            return next != null && nextPos < entrysC && (to == null || next.key().compareTo(to) < 0);
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            BaseEntry<ByteBuffer> current = next;
            nextPos++;
            if (nextPos < entrysC) {
                next = readEntry(nextPos);
            }
            return current;
        }
    }
}

