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
import java.nio.file.attribute.FileTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

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

    private static int sizeOfEntry(BaseEntry<ByteBuffer> entry) {
        return 2 * Integer.BYTES + entry.key().capacity() + entry.value().capacity();
    }

    private static int getFileNumber(Path pathToFile) {
        String number = pathToFile.getFileName().toString()
                .replaceFirst(DATA_FILE_HEADER, "")
                .replaceFirst(FILE_EXTENSION, "");
        return Integer.parseInt(number);
    }

    private static int readInt(FileChannel in, ByteBuffer temp) throws IOException {
        temp.clear();
        in.read(temp);
        return temp.flip().getInt();
    }

    private static long readLong(FileChannel in, ByteBuffer temp) throws IOException {
        temp.clear();
        in.read(temp);
        return temp.flip().getLong();
    }

    private static ByteBuffer readBuffer(FileChannel in, int size) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        in.read(buffer);
        return buffer.flip();
    }

    private static ByteBuffer readBuffer(FileChannel in, long pos, ByteBuffer temp) throws IOException {
        in.position(pos);
        return readBuffer(in, readInt(in, temp));
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        PriorityQueue<PeekIterator<BaseEntry<ByteBuffer>>> heap =
                new PriorityQueue<>(Comparator.comparing(o -> o.peek().key()));
        Iterator<BaseEntry<ByteBuffer>> temp = getInMemoryIterator(from, to);
        if (temp.hasNext()) {
            heap.add(new PeekIterator<>(temp, FileTime.fromMillis(Long.MAX_VALUE)));
        }
        for (Path searchPath : getPaths()) {
            temp = getInFileIterator(searchPath, from, to);
            if (temp.hasNext()) {
                heap.add(new PeekIterator<>(temp, Files.getLastModifiedTime(searchPath)));
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
                list.add(new BaseEntry<>(readBuffer(in, indexes[i], temp), readBuffer(in, readInt(in, temp))));
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
            if (comparison > 0) {
                min = mid + 1;
                continue;
            } else if (comparison < 0) {
                max = mid;
                continue;
            }
            return mid;
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
        if (value != null) {
            return value;
        }
        if (Files.exists(path)) {
            FileTime lastMod = FileTime.fromMillis(0);
            for (Path searchPath : getPaths()) {
                if (lastMod.compareTo(Files.getLastModifiedTime(searchPath)) < 0) {
                    value = binarySearchFile(searchPath, key);
                    if (value != null) {
                        lastMod = Files.getLastModifiedTime(searchPath);
                    }
                }
            }
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
            ByteBuffer bound;
            while (min <= max) {
                bound = readBuffer(in, indexes[min], temp);
                comparison = key.compareTo(bound);
                if (key.compareTo(bound) < 0) {
                    return null;
                }
                if (comparison != 0) {
                    bound = readBuffer(in, indexes[max], temp);
                    comparison = key.compareTo(bound);
                    if (comparison > 0) {
                        return null;
                    }
                    if (comparison != 0) {
                        mid = min + (max - min) / 2;
                        bound = readBuffer(in, indexes[mid], temp);
                        comparison = key.compareTo(bound);
                        if (comparison > 0) {
                            min = mid + 1;
                            continue;
                        } else if (comparison < 0) {
                            max = mid - 1;
                            continue;
                        }
                    }
                }

                return new BaseEntry<>(key, readBuffer(in, readInt(in, temp)));
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
                }

                buffer.putInt(entry.key().capacity()).put(entry.key());
                buffer.putInt(entry.value().capacity()).put(entry.value());
                buffer.flip();

                indexBuffer.putLong(dataOut.position());
                dataOut.write(buffer);

                buffer.clear();
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

    private static class PeekIterator<E> implements Iterator<E> {

        private final Iterator<E> iter;

        private final FileTime lastModification;

        private E value;

        private PeekIterator(Iterator<E> iter, FileTime lastModification) {
            this.iter = iter;
            this.lastModification = lastModification;
        }

        public E peek() {
            if (value == null) {
                value = iter.next();
            }
            return value;
        }

        @Override
        public boolean hasNext() {
            return value != null || iter.hasNext();
        }

        @Override
        public E next() {
            E peek = peek();
            value = null;
            return peek;
        }

    }

    private record DaoIterator(PriorityQueue<PeekIterator<BaseEntry<ByteBuffer>>> heap)
            implements Iterator<BaseEntry<ByteBuffer>> {

        @Override
        public boolean hasNext() {
            return heap.peek() != null && heap.peek().hasNext();
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            PeekIterator<BaseEntry<ByteBuffer>> iter = heap.poll();
            BaseEntry<ByteBuffer> entry = iter.next();
            if (iter.hasNext()) {
                heap.add(iter);
            }
            PeekIterator<BaseEntry<ByteBuffer>> nextIter;
            while (hasNext() && entry.key().compareTo(heap.peek().peek().key()) == 0) {
                nextIter = heap.poll();
                if (iter.lastModification.compareTo(nextIter.lastModification) < 0) {
                    entry = nextIter.next();
                } else {
                    nextIter.next();
                }
                if (nextIter.hasNext()) {
                    heap.add(nextIter);
                }
            }

            return entry;
        }
    }
}
