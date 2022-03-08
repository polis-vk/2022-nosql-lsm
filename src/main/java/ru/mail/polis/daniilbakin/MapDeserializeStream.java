package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;

public class MapDeserializeStream {

    private final MappedByteBuffer mapBuffer;
    private final MappedByteBuffer indexesBuffer;

    private static final Method CLEAN;

    static {
        try {
            Class<?> aClass = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = aClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException();
        }
    }

    public MapDeserializeStream(Path map, Path indexes) throws IOException {
        FileChannel mapChannel = (FileChannel) Files.newByteChannel(map, Set.of(StandardOpenOption.READ));
        FileChannel indexesChannel = (FileChannel) Files.newByteChannel(indexes, Set.of(StandardOpenOption.READ));

        mapBuffer = mapChannel.map(FileChannel.MapMode.READ_ONLY, 0, mapChannel.size());
        indexesBuffer = indexesChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexesChannel.size());

        mapChannel.close();
        indexesChannel.close();
    }

    public void close() throws IOException {
        unmap(mapBuffer);
        unmap(indexesBuffer);
    }

    public BaseEntry<ByteBuffer> readByKey(ByteBuffer key) {
        return binarySearch(key);
    }

    private BaseEntry<ByteBuffer> binarySearch(ByteBuffer key) {
        int first = 0;
        int last = indexesBuffer.capacity() / Integer.BYTES;
        int position = (first + last) / 2;

        ByteBuffer currKey = readByteBuffer(getIndexByOrder(position));
        int compare = currKey.compareTo(key);
        while ((compare != 0) && (first <= last)) {
            if (compare > 0) {
                last = position - 1;
            } else {
                first = position + 1;
            }
            position = (first + last) / 2;
            currKey = readByteBuffer(getIndexByOrder(position));
            compare = currKey.compareTo(key);
        }
        if (first <= last) {
            return readEntry(getIndexByOrder(position));
        }
        return null;
    }

    /**
     * Position in bytes.
     */
    private BaseEntry<ByteBuffer> readEntry(int position) {
        ByteBuffer key = readByteBuffer(position);
        ByteBuffer value = readByteBuffer(position + key.capacity() + Integer.BYTES);
        return new BaseEntry<>(key.duplicate(), value);
    }

    /**
     * Position in bytes.
     */
    private ByteBuffer readByteBuffer(int position) {
        int length = readInt(position);
        return mapBuffer.slice(position + Integer.BYTES, length);
    }

    /**
     * Position in bytes.
     */
    private Integer readInt(int position) {
        return mapBuffer.getInt(position);
    }

    private int getIndexByOrder(int order) {
        return indexesBuffer.getInt(order * Integer.BYTES);
    }

    private void unmap(MappedByteBuffer nmap) throws IOException {
        if (nmap == null) {
            return;
        }
        try {
            CLEAN.invoke(null, nmap);
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new IOException();
        }
    }

}
