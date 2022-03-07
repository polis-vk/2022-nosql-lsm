package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.LongStream;

/**
 * Author: Dmitry Kondraev.
 */

public class FileBackedDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {

    private static final String INDEX_FILENAME = "index";
    private static final String DATA_FILENAME = "data";

    private static final Comparator<MemorySegment> lexicographically = (lhs, rhs) -> {
        // lexicographic comparison of UTF-8 strings can be done by byte, according to RFC 3239
        // (https://www.rfc-editor.org/rfc/rfc3629.txt, page 2)

        // this string comparison likely won't work with collation different from ASCII
        long offset = lhs.mismatch(rhs);
        if (offset == -1) {
            return 0;
        }
        if (offset == lhs.byteSize()) {
            return -1;
        }
        if (offset == rhs.byteSize()) {
            return 1;
        }
        return Byte.compare(
                MemoryAccess.getByteAtOffset(lhs, offset),
                MemoryAccess.getByteAtOffset(rhs, offset)
        );
    };

    private final Config config;

    private final ConcurrentNavigableMap<MemorySegment, BaseEntry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(lexicographically);

    private MemorySegment mappedData;
    private final ResourceScope dataScope = ResourceScope.newConfinedScope();
    private long[] offsets;

    public FileBackedDao(Config config) {
        this.config = config;
    }

    public FileBackedDao() {
        this(null);
    }

    private static <K, V> Iterator<V> iterator(Map<K, V> map) {
        return map.values().iterator();
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return iterator(map);
        }
        if (from == null) {
            return iterator(map.headMap(to));
        }
        if (to == null) {
            return iterator(map.tailMap(from));
        }
        return iterator(map.subMap(from, to));
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        // implicit check for non-null entry and entry.key()
        map.put(entry.key(), entry);
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) {
        BaseEntry<MemorySegment> result = map.get(key);
        if (result != null) {
            return result;
        }
        if (isInMemoryOnly()) {
            try {
                offsets = readOffsets();
                mappedData = mappedFile(config.basePath().resolve(DATA_FILENAME));
            } catch (NoSuchFileException e) {
                return null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return binarySearch(key);
    }

    private BaseEntry<MemorySegment> binarySearch(MemorySegment key) {
        int low = 0;
        int high = entriesMapped() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            MemorySegment midVal = mappedKey(mid);
            if (lexicographically.compare(midVal, key) < 0) {
                low = mid + 1;
            } else if (lexicographically.compare(midVal, key) > 0) {
                high = mid - 1;
            } else {
                return new BaseEntry<>(midVal, mappedValue(mid)); // key found
            }
        }
        return null; // key not found.
    }

    private long[] calculateOffsets() {
        long[] result = LongStream.concat(
                LongStream.of(0L),
                map
                        .values()
                        .stream()
                        // alternating key and value sizes
                        .flatMapToLong(entry -> LongStream.of(entry.key().byteSize(), entry.value().byteSize()))
        ).toArray();
        Arrays.parallelPrefix(result, Long::sum);
        return result;
    }

    private long[] readOffsets() throws IOException {
        try (DataInputStream in = inputOf(indexFile())) {
            int size = in.readInt();
            long[] result = new long[size];
            for (int i = 0; i < result.length; i++) {
                result[i] = in.readLong();
            }
            return result;
        }
    }

    private Path indexFile() {
        return config.basePath().resolve(INDEX_FILENAME);
    }

    private long keyOffset(int i) {
        return offsets[i << 1];
    }

    private long valueOffset(int i) {
        return offsets[(i << 1) | 1];
    }

    private long keySize(int i) {
        return valueOffset(i) - keyOffset(i);
    }

    private long valueSize(int i) {
        return keyOffset(i + 1) - valueOffset(i);
    }

    private int entriesMapped() {
        return offsets.length >> 1;
    }

    private MemorySegment mappedKey(int i) {
        return mappedData.asSlice(keyOffset(i), keySize(i));
    }

    private MemorySegment mappedValue(int i) {
        return mappedData.asSlice(valueOffset(i), valueSize(i));
    }

    private static DataOutputStream outputOf(Path file) throws IOException {
        return new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file)));
    }

    private static DataInputStream inputOf(Path file) throws IOException {
        return new DataInputStream(new BufferedInputStream(Files.newInputStream(file)));
    }

    private static Path createFileIfNotExists(Path path) throws IOException {
        try {
            return Files.createFile(path);
        } catch (FileAlreadyExistsException ignored) {
            return path;
        }
    }

    private MemorySegment mappedFile(Path path) throws IOException {
        return MemorySegment.mapFile(
                path,
                0L,
                offsets[offsets.length - 1],
                FileChannel.MapMode.READ_WRITE,
                dataScope);
    }

    private boolean isInMemoryOnly() {
        // !isInMemoryOnly() implies map.isEmpty()
        // could be also offsets == null
        return mappedData == null;
    }

    @Override
    public void flush() throws IOException {
        // write offsets array to index file in format:
        // ┌─────────┬───────────────────┐
        // │size: int│offsets: long[size]│
        // └─────────┴───────────────────┘
        try (DataOutputStream out = outputOf(indexFile())) {
            offsets = calculateOffsets();
            out.writeInt(offsets.length);
            for (long offset : offsets) {
                out.writeLong(offset);
            }
        }
        if (isInMemoryOnly()) {
            mappedData = mappedFile(createFileIfNotExists(config.basePath().resolve(DATA_FILENAME)));
        }
        // write key-value pairs in format:
        // ┌─────────────────────┬─────────────────────────┐
        // │key: byte[keySize(i)]│value: byte[valueSize(i)]│ entriesMapped() times
        // └─────────────────────┴─────────────────────────┘
        int i = 0;
        for (BaseEntry<MemorySegment> entry : map.values()) {
            mappedKey(i).copyFrom(entry.key());
            mappedValue(i).copyFrom(entry.value());
            i++;
        }
        map.clear();
        mappedData.unload();
        offsets = null;
    }

    @Override
    public void close() throws IOException {
        Dao.super.close();
        dataScope.close();
    }
}
