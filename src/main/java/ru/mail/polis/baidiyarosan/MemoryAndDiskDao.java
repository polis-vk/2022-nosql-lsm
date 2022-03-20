package ru.mail.polis.baidiyarosan;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

import static ru.mail.polis.baidiyarosan.FileUtils.readBuffer;
import static ru.mail.polis.baidiyarosan.FileUtils.readLong;
import static ru.mail.polis.baidiyarosan.FileUtils.sizeOfEntry;
import static ru.mail.polis.baidiyarosan.FileUtils.writeEntryToBuffer;

public class MemoryAndDiskDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private static final String DATA_FILE_HEADER = "data";

    private static final String INDEX_FOLDER = "indexes";

    private static final String INDEX_FILE_HEADER = "index";

    private static final String FILE_EXTENSION = ".log";

    private final NavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> collection = new ConcurrentSkipListMap<>();

    private final Path path;

    public MemoryAndDiskDao(Config config) throws IOException {
        this.path = config.basePath();
        Path indexesDir = path.resolve(Paths.get(INDEX_FOLDER));
        if (Files.exists(path) && Files.notExists(indexesDir)) {
            Files.createDirectory(indexesDir);
        }

    }

    private static int getFileNumber(Path pathToFile) {
        String number = pathToFile.getFileName().toString()
                .replaceFirst(DATA_FILE_HEADER, "")
                .replaceFirst(FILE_EXTENSION, "");
        return Integer.parseInt(number);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        PriorityQueue<PeekIterator<BaseEntry<ByteBuffer>>> heap =
                new PriorityQueue<>(Comparator.comparing(o -> o.peek().key()));
        Iterator<BaseEntry<ByteBuffer>> temp = getInMemoryIterator(from, to);
        if (temp.hasNext()) {
            heap.add(new PeekIterator<>(temp, Integer.MAX_VALUE));
        }
        for (Path searchPath : getPaths()) {
            temp = getInFileIterator(searchPath, from, to);
            if (temp.hasNext()) {
                heap.add(new PeekIterator<>(temp, getFileNumber(searchPath)));
            }
        }

        return new DaoIterator(heap);
    }

    private Iterator<BaseEntry<ByteBuffer>> getInFileIterator(Path filePath, ByteBuffer from, ByteBuffer to)
            throws IOException {
        try (FileChannel in = FileChannel.open(filePath, StandardOpenOption.READ)) {
            List<BaseEntry<ByteBuffer>> list = new LinkedList<>();
            int file = getFileNumber(filePath);
            long[] indexes = getIndexArray(file);
            ByteBuffer temp = ByteBuffer.allocate(Integer.BYTES);
            int start = 0;
            int end = indexes.length - 1;
            if (from != null) {
                start = getStartIndex(in, indexes, from, temp);
            }
            if (to != null) {
                end = getEndIndex(in, indexes, to, temp);
            }
            if (start == -1 || end == -1) {
                return Collections.emptyIterator();
            }

            for (int i = start; i <= end; ++i) {
                list.add(new BaseEntry<>(readBuffer(in, indexes[i], temp), readBuffer(in, temp)));
            }
            return list.iterator();
        }
    }

    private int getStartIndex(FileChannel in, long[] indexes, ByteBuffer key, ByteBuffer temp)
            throws IOException {
        int min = 0;
        int max = indexes.length - 1;
        int mid;
        int comparison;
        while (min <= max) {
            if (key.compareTo(readBuffer(in, indexes[min], temp)) <= 0) {
                return min;
            }
            comparison = key.compareTo(readBuffer(in, indexes[max], temp));
            if (comparison > 0) {
                return -1;
            }
            if (comparison == 0) {
                return max;
            }
            mid = min + (max - min) / 2;
            comparison = key.compareTo(readBuffer(in, indexes[mid], temp));
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

    private int getEndIndex(FileChannel in, long[] indexes, ByteBuffer key, ByteBuffer temp)
            throws IOException {
        int min = 0;
        int max = indexes.length - 1;
        int mid;
        while (min <= max) {
            if (key.compareTo(readBuffer(in, indexes[min], temp)) <= 0) {
                return -1;
            }
            if (key.compareTo(readBuffer(in, indexes[max], temp)) > 0) {
                return max;
            }
            mid = min + 1 + (max - min) / 2;
            if (key.compareTo(readBuffer(in, indexes[mid], temp)) > 0) {
                min = mid;
            } else {
                max = mid - 1;
            }
        }
        return min;
    }

    private Iterator<BaseEntry<ByteBuffer>> getInMemoryIterator(ByteBuffer from, ByteBuffer to) {
        if (collection.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (from == null && to == null) {
            return collection.values().iterator();
        }
        final boolean flag = (to == null || !to.equals(collection.floorKey(to)));
        from = (from == null ? collection.firstKey() : collection.ceilingKey(from));
        to = (to == null ? collection.lastKey() : collection.floorKey(to));

        if (from == null || to == null || from.compareTo(to) > 0) {
            return Collections.emptyIterator();
        }
        return collection.subMap(from, true, to, flag).values().iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        collection.put(entry.key(), entry);
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) throws IOException {
        BaseEntry<ByteBuffer> value = collection.get(key);
        if (value == null && Files.exists(path)) {
            int order = 0;
            for (Path searchPath : getPaths()) {
                if (order < getFileNumber(searchPath)) {
                    value = binarySearchFile(searchPath, key);
                    if (value != null) {
                        order = getFileNumber(searchPath);
                    }
                }
            }
        }
        if (value != null && value.value() == null) {
            return null;
        }
        return value;
    }

    private BaseEntry<ByteBuffer> binarySearchFile(Path searchPath, ByteBuffer key)
            throws IOException {
        try (FileChannel in = FileChannel.open(searchPath, StandardOpenOption.READ)) {
            int file = getFileNumber(searchPath);
            long[] indexes = getIndexArray(file);
            ByteBuffer temp = ByteBuffer.allocate(Integer.BYTES);
            int min = 0;
            int max = indexes.length - 1;
            int mid;
            int comparison;
            while (min <= max) {
                comparison = key.compareTo(readBuffer(in, indexes[min], temp));
                if (comparison < 0) {
                    return null;
                }
                if (comparison == 0) {
                    return new BaseEntry<>(key, readBuffer(in, temp));
                }

                comparison = key.compareTo(readBuffer(in, indexes[max], temp));
                if (comparison > 0) {
                    return null;
                }
                if (comparison == 0) {
                    return new BaseEntry<>(key, readBuffer(in, temp));
                }

                mid = min + (max - min) / 2;
                comparison = key.compareTo(readBuffer(in, indexes[mid], temp));
                if (comparison == 0) {
                    return new BaseEntry<>(key, readBuffer(in, temp));
                }
                if (comparison > 0) {
                    min = mid + 1;
                } else {
                    max = mid - 1;
                }
            }
        }
        return null;
    }

    @Override
    public void flush() throws IOException {
        if (collection.isEmpty()) {
            return;
        }
        int size;
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{});
        ByteBuffer indexBuffer = ByteBuffer.allocate(collection.size() * Long.BYTES);
        int fileNumber = getLastFileNumber() + 1;
        Path dataPath = path.resolve(DATA_FILE_HEADER + fileNumber + FILE_EXTENSION);
        Path indexPath = path.resolve(Paths.get(INDEX_FOLDER,
                INDEX_FILE_HEADER + fileNumber + FILE_EXTENSION));
        try (FileChannel dataOut = FileChannel.open(dataPath,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
             FileChannel indexOut = FileChannel.open(indexPath,
                     StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            for (BaseEntry<ByteBuffer> entry : collection.values()) {
                size = sizeOfEntry(entry);
                if (buffer.capacity() < size) {
                    buffer = ByteBuffer.allocate(size);
                } else {
                    buffer.clear();
                }

                indexBuffer.putLong(dataOut.position());
                dataOut.write(writeEntryToBuffer(buffer, entry));
            }
            indexBuffer.flip();
            indexOut.write(indexBuffer);

        }
    }

    private List<Path> getPaths() throws IOException {
        List<Path> paths;
        try (Stream<Path> s = Files.list(path)) {
            paths = s.filter(Files::isRegularFile).toList();
        }
        return paths;
    }

    private long[] getIndexArray(int fileNumber) throws IOException {
        long[] array;
        Path indexPath = path.resolve(Paths.get(INDEX_FOLDER,
                INDEX_FILE_HEADER + fileNumber + FILE_EXTENSION));
        try (FileChannel indexOut = FileChannel.open(indexPath, StandardOpenOption.READ)) {
            int size = (int) (indexOut.size() / Long.BYTES);
            array = new long[size];
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            for (int i = 0; i < size; ++i) {
                array[i] = readLong(indexOut, buffer);
            }
        }

        return array;
    }

    private int getLastFileNumber() throws IOException {
        return getPaths().size();
    }

}
