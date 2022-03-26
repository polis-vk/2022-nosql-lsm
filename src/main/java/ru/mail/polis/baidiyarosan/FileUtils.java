package ru.mail.polis.baidiyarosan;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.stream.Stream;

public final class FileUtils {

    public static final int NULL_SIZE_FLAG = -1;

    public static final String DATA_FILE_HEADER = "data";

    public static final String INDEX_FOLDER = "indexes";

    public static final String INDEX_FILE_HEADER = "index";

    public static final String FILE_EXTENSION = ".log";

    private FileUtils() {
        // Utility class
    }

    public static List<Path> getPaths(Path path) throws IOException {
        try (Stream<Path> s = Files.list(path)) {
            return s.filter(Files::isRegularFile).toList();
        }
    }

    public static Path getDataPath(Path directory, int fileNumber) {
        return directory.resolve(DATA_FILE_HEADER + fileNumber + FILE_EXTENSION);
    }

    public static Path getIndexPath(Path directory, int fileNumber) {
        return directory.resolve(Paths.get(INDEX_FOLDER, INDEX_FILE_HEADER + fileNumber + FILE_EXTENSION));
    }

    public static int sizeOfEntry(BaseEntry<ByteBuffer> entry) {
        return 2 * Integer.BYTES + entry.key().remaining() + (entry.value() == null ? 0 : entry.value().remaining());
    }

    public static ByteBuffer readMappedBuffer(MappedByteBuffer in, int pos) {
        int size = in.getInt(pos);
        return in.slice(pos + Integer.BYTES, size);
    }

    public static BaseEntry<ByteBuffer> readMappedEntry(MappedByteBuffer in, int pos) {
        int keySize = in.getInt(pos);
        ByteBuffer key = in.slice(pos + Integer.BYTES, keySize);
        int valSize = in.getInt(pos + Integer.BYTES + keySize);
        if (valSize == NULL_SIZE_FLAG) {
            return new BaseEntry<>(key, null);
        }
        ByteBuffer val = in.slice(pos + 2 * Integer.BYTES + keySize, valSize);
        return new BaseEntry<>(key, val);
    }

    public static int intAt(MappedByteBuffer indexes, int position) {
        return indexes.getInt(position * Integer.BYTES);
    }

    private static ByteBuffer writeEntryToBuffer(ByteBuffer buffer, BaseEntry<ByteBuffer> entry) {
        buffer.putInt(entry.key().remaining()).put(entry.key());
        if (entry.value() == null) {
            buffer.putInt(NULL_SIZE_FLAG);
        } else {
            buffer.putInt(entry.value().remaining()).put(entry.value());
        }
        return buffer.flip();
    }

    public static void writeOnDisk(NavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> collection, Path path) throws IOException {
        int size;
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{});
        ByteBuffer indexBuffer = ByteBuffer.allocate(Integer.BYTES);
        int fileNumber = getPaths(path).size() + 1;
        try (FileChannel dataOut = FileChannel.open(getDataPath(path, fileNumber), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE); FileChannel indexOut = FileChannel.open(getIndexPath(path, fileNumber), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            for (BaseEntry<ByteBuffer> entry : collection.values()) {
                size = sizeOfEntry(entry);
                if (buffer.remaining() < size) {
                    buffer = ByteBuffer.allocate(size);
                } else {
                    buffer.clear();
                }
                indexBuffer.clear();
                indexBuffer.putInt((int) dataOut.position());
                indexBuffer.flip();
                indexOut.write(indexBuffer);
                dataOut.write(writeEntryToBuffer(buffer, entry));
            }
        }
    }

    public static Iterator<BaseEntry<ByteBuffer>> getInMemoryIterator(NavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> collection, ByteBuffer from, ByteBuffer to) {
        if (collection.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (from == null && to == null) {
            return collection.values().iterator();
        }

        ByteBuffer start = (from == null ? collection.firstKey() : collection.ceilingKey(from));
        ByteBuffer end = (to == null ? collection.lastKey() : collection.floorKey(to));

        if (start == null || end == null || start.compareTo(end) > 0) {
            return Collections.emptyIterator();
        }
        return collection.subMap(start, true, end, to == null || !to.equals(collection.floorKey(to))).values().iterator();
    }

    public static Iterator<BaseEntry<ByteBuffer>> getInFileIterator(MappedByteBuffer file, MappedByteBuffer index, ByteBuffer from, ByteBuffer to) {

        int start = 0;
        int end = index.capacity() / Integer.BYTES - 1;
        if (from != null) {
            start = getStartIndex(file, index, from, start, end);
        }

        if (start == -1) {
            return Collections.emptyIterator();
        }

        if (to != null) {
            end = getEndIndex(file, index, to, start, end);
        }
        if (end == -1) {
            return Collections.emptyIterator();
        }

        List<BaseEntry<ByteBuffer>> list = new LinkedList<>();
        for (int i = start; i <= end; ++i) {
            list.add(readMappedEntry(file, intAt(index, i)));
        }
        return list.iterator();
    }

    public static int getStartIndex(MappedByteBuffer in, MappedByteBuffer indexes, ByteBuffer key, int start, int end) {
        int min = start;
        int max = end;
        int mid;
        int comparison;
        while (min <= max) {
            if (key.compareTo(readMappedBuffer(in, intAt(indexes, min))) <= 0) {
                return min;
            }
            comparison = key.compareTo(readMappedBuffer(in, intAt(indexes, max)));
            if (comparison > 0) {
                return -1;
            }
            if (comparison == 0) {
                return max;
            }
            mid = min + (max - min) / 2;
            comparison = key.compareTo(readMappedBuffer(in, intAt(indexes, mid)));
            if (comparison == 0) {
                return mid;
            }
            if (comparison > 0) {
                min = mid + 1;
            } else {
                max = mid;
            }
        }
        return max;
    }

    public static int getEndIndex(MappedByteBuffer in, MappedByteBuffer indexes, ByteBuffer key, int start, int end) {
        int min = start;
        int max = end;
        int mid;
        while (min <= max) {

            if (key.compareTo(readMappedBuffer(in, intAt(indexes, min))) <= 0) {
                return -1;
            }
            if (key.compareTo(readMappedBuffer(in, intAt(indexes, max))) > 0) {
                return max;
            }

            mid = min + 1 + (max - min) / 2;
            if (key.compareTo(readMappedBuffer(in, intAt(indexes, mid))) > 0) {
                min = mid;
            } else {
                max = mid - 1;
            }
        }
        return min;
    }

}
