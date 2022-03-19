package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class MapsDeserializeStream {

    private final MappedByteBuffer[] mapData;
    private final MappedByteBuffer[] indexesData;
    private final int dataCount;
    private final Method unmap;
    private final Object fieldValue;

    public MapsDeserializeStream(Config config, int dataCount) throws IOException {
        this.dataCount = dataCount;
        mapData = new MappedByteBuffer[dataCount];
        indexesData = new MappedByteBuffer[dataCount];
        for (int i = 0; i < dataCount; i++) {
            Path mapPath = config.basePath().resolve("myLog" + (dataCount - i - 1));
            Path indexesPath = config.basePath().resolve("indexes" + (dataCount - i - 1));

            FileChannel mapChannel = (FileChannel) Files.newByteChannel(mapPath, Set.of(StandardOpenOption.READ));
            FileChannel indexesChannel = (FileChannel)
                    Files.newByteChannel(indexesPath, Set.of(StandardOpenOption.READ));

            mapData[i] = mapChannel.map(FileChannel.MapMode.READ_ONLY, 0, mapChannel.size());
            indexesData[i] = indexesChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexesChannel.size());

            mapChannel.close();
            indexesChannel.close();
        }

        try {
            unmap = Class.forName("sun.misc.Unsafe").getMethod("invokeCleaner", ByteBuffer.class);
            unmap.setAccessible(true);
            Field theUnsafeField = Class.forName("sun.misc.Unsafe").getDeclaredField("theUnsafe");
            theUnsafeField.setAccessible(true);
            fieldValue = theUnsafeField.get(null);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void close() throws IOException {
        try {
            for (int i = 0; i < dataCount; i++) {
                unmap.invoke(fieldValue, mapData[i]);
                unmap.invoke(fieldValue, indexesData[i]);
            }
        } catch (ReflectiveOperationException e) {
            throw new IOException(e);
        }
    }

    public BaseEntry<ByteBuffer> readByKey(ByteBuffer key) {
        for (int i = 0; i < dataCount; i++) {
            BaseEntry<ByteBuffer> entry = readByKey(key, i);
            if (entry == null) {
                continue;
            }
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }
        return null;
    }

    public Iterator<BaseEntry<ByteBuffer>> getRange(
            ByteBuffer from, ByteBuffer to, PeekIterator<BaseEntry<ByteBuffer>> dataIterator
    ) {
        List<PeekIterator<BaseEntry<ByteBuffer>>> iterators = new ArrayList<>();
        iterators.add(dataIterator);
        for (int i = 0; i < dataCount; i++) {
            iterators.add(getIterator(from, to, indexesData[i], mapData[i]));
        }
        return new MergeIterator<>(iterators);
    }

    private PeekIterator<BaseEntry<ByteBuffer>> getIterator(
            ByteBuffer from, ByteBuffer to, MappedByteBuffer indexesBuffer, MappedByteBuffer mapBuffer
    ) {
        if (indexesBuffer.capacity() < Integer.BYTES) {
            return new PeekIterator<>(Collections.emptyIterator());
        }
        int startIndex;
        int endIndex;

        if (from == null) {
            startIndex = 0;
        } else {
            startIndex = binarySearchIndex(from, indexesBuffer, mapBuffer, true);
        }

        if (to == null) {
            endIndex = indexesBuffer.capacity() / Integer.BYTES;
        } else {
            endIndex = binarySearchIndex(to, indexesBuffer, mapBuffer, true);
        }

        return new PeekIterator<>(new Iterator<>() {
            int next = startIndex;
            final long last = endIndex;

            @Override
            public boolean hasNext() {
                return next < last;
            }

            @Override
            public BaseEntry<ByteBuffer> next() {
                return readEntry(getInternalIndexByOrder(next++, indexesBuffer), mapBuffer);
            }
        });
    }

    private BaseEntry<ByteBuffer> readByKey(ByteBuffer key, int index) {
        MappedByteBuffer indexesBuffer = indexesData[index];
        if (indexesBuffer.capacity() < Integer.BYTES) {
            return null;
        }

        MappedByteBuffer mapBuffer = mapData[index];
        int keyIndex = binarySearchIndex(key, indexesBuffer, mapBuffer, false);
        if (keyIndex == -1) {
            return null;
        }
        return readEntry(getInternalIndexByOrder(keyIndex, indexesBuffer), mapBuffer);
    }

    private int binarySearchIndex(
            ByteBuffer key, MappedByteBuffer indexesBuffer, MappedByteBuffer mapBuffer, boolean needClosestRight
    ) {
        int size = indexesBuffer.capacity() / Integer.BYTES;
        int first = 0;
        int last = size;
        int position = (first + last) / 2;

        ByteBuffer currKey = readByteBuffer(getInternalIndexByOrder(position, indexesBuffer), mapBuffer);
        assert currKey != null;

        int compare = currKey.compareTo(key);
        while ((compare != 0) && (first <= last)) {
            if (compare > 0) {
                last = position - 1;
            } else {
                first = position + 1;
            }

            position = (first + last) / 2;
            if (position == size) {
                break;
            }
            currKey = readByteBuffer(getInternalIndexByOrder(position, indexesBuffer), mapBuffer);
            assert currKey != null;
            compare = currKey.compareTo(key);
        }
        if (first <= last) {
            return position;
        }
        if (needClosestRight) {
            if (compare > 0) {
                return position;
            } else {
                return position + 1;
            }
        }
        return -1;
    }

    /**
     * Position in bytes.
     */
    private BaseEntry<ByteBuffer> readEntry(int position, MappedByteBuffer mapBuffer) {
        ByteBuffer key = readByteBuffer(position, mapBuffer);
        assert key != null;
        ByteBuffer value = readByteBuffer(position + key.capacity() + Integer.BYTES, mapBuffer);
        return new BaseEntry<>(key.duplicate(), value);
    }

    /**
     * Position in bytes.
     */
    private ByteBuffer readByteBuffer(int position, MappedByteBuffer mapBuffer) {
        int length = readInt(position, mapBuffer);
        if (length == -1) {
            return null;
        }
        return mapBuffer.slice(position + Integer.BYTES, length);
    }

    /**
     * Position in bytes.
     */
    private Integer readInt(int position, MappedByteBuffer mapBuffer) {
        return mapBuffer.getInt(position);
    }

    private int getInternalIndexByOrder(int order, MappedByteBuffer indexesBuffer) {
        return indexesBuffer.getInt(order * Integer.BYTES);
    }

}
