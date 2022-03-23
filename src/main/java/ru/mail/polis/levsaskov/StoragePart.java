package ru.mail.polis.levsaskov;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.NavigableMap;
import java.util.TreeMap;

public class StoragePart {
    private static final int BYTES_IN_INT = 4;
    private static final int BYTES_IN_LONG = 8;

    private MappedByteBuffer memoryBB;
    private MappedByteBuffer indexBB;
    private int entrysC;

    public void init(Path memoryPath, Path indexPath) throws IOException {
        memoryBB = mapFile(memoryPath);
        indexBB = mapFile(indexPath);
        entrysC = indexBB.capacity() / BYTES_IN_LONG;
    }

    public BaseEntry<ByteBuffer> get(ByteBuffer key) {
        //TODO: Correction from stage 2
        int position = binarySearch(entrysC - 1, key);
        BaseEntry<ByteBuffer> res = readEntry(position);
        return res.key().equals(key) ? res : null;
    }

    //    public PeekIterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
    public NavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        // TODO: return peekIterator
        NavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> res = new TreeMap<>();
        int pos = binarySearch(entrysC - 1, from);
        BaseEntry<ByteBuffer> entry;

        // Граничные случаи
        if (pos + 1 < entrysC && from != null
                && readEntry(pos).key().compareTo(from) < 0) {
            pos++;
        }

        if (from == null || readEntry(pos).key().compareTo(from) >= 0) {
            while (pos < entrysC) {
                entry = readEntry(pos);
                if (to != null && entry.key().compareTo(to) >= 0) {
                    break;
                }

                res.put(entry.key(), entry);
                pos++;
            }
        }

        return res;
    }

    public void close() {
        unmap(indexBB);
        indexBB = null;
        unmap(memoryBB);
        memoryBB = null;
    }

    private int binarySearch(int inLast, ByteBuffer key) {
        //TODO: correction from stage 2
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
        // TODO: Переделать запись на инты
        int ind = (int) indexBB.getLong(entryN * BYTES_IN_LONG);
        byte[] key = readBytes(ind);
        ind += BYTES_IN_INT + key.length;
        byte[] value = readBytes(ind);
        return new BaseEntry<>(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
    }

    /**
     * Read integer and bytes, how many was in this integer, from memoryBB.
     * Reading begins from ind.
     *
     * @param ind
     * @return
     */
    private byte[] readBytes(int ind) {
        int len = memoryBB.getInt(ind);
        ind += BYTES_IN_INT;
        byte[] bytes = new byte[len];
        memoryBB.get(ind, bytes);
        return bytes;
    }

    private static MappedByteBuffer mapFile(Path filePath) throws IOException {

        MappedByteBuffer mappedFile;
        try (
                FileChannel fileChannel = (FileChannel) Files.newByteChannel(filePath, EnumSet.of(StandardOpenOption.READ))
        ) {
            mappedFile = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        }

        //LOG
        System.out.println(mappedFile);
        System.out.println("Mapped!");

        return mappedFile;
    }

    private static void unmap(MappedByteBuffer buffer) {
        try {
            //LOG
            System.out.println(buffer);

            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Object unsafe = unsafeField.get(null);
            Method invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
            invokeCleaner.invoke(unsafe, buffer);
            System.out.println("Unmapped!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
