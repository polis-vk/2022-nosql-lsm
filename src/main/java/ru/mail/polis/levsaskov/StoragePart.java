package ru.mail.polis.levsaskov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

public class StoragePart implements AutoCloseable {
    public static final int LEN_FOR_NULL = -1;
    private static final int DEFAULT_ALLOC_SIZE = 2048;
    private static final int INDEX_BOOST_PORTION = 200;
    private static final int MEMORY_BOOST_PORTION = 5000;
    private static final int MIN_MAP_SIZE = 0;

    private final int storagePartN;
    private final Path indexPath;
    private final Path memoryPath;
    private MappedByteBuffer indexBB;
    private MappedByteBuffer memoryBB;
    private int entrysC;

    private StoragePart(Path indexPath, Path memoryPath,
                        MappedByteBuffer indexBB, MappedByteBuffer memoryBB, int storagePartN) {
        this.indexPath = indexPath;
        this.memoryPath = memoryPath;
        this.storagePartN = storagePartN;
        this.memoryBB = memoryBB;
        this.indexBB = indexBB;
        // I write count of written entrys in the end of index file
        if (indexBB.capacity() != 0) {
            entrysC = indexBB.getInt(indexBB.capacity() - Integer.BYTES);
        }
    }

    public static StoragePart load(Path indexPath, Path memoryPath, int storagePartN) throws IOException {
        MappedByteBuffer indexBB = mapFile(indexPath);
        MappedByteBuffer memoryBB = mapFile(memoryPath);
        return new StoragePart(indexPath, memoryPath, indexBB, memoryBB, storagePartN);
    }

    public Entry<ByteBuffer> get(ByteBuffer key) {
        int position = getGreaterOrEqual(entrysC - 1, key);
        Entry<ByteBuffer> res = readEntry(position);
        return res.key().equals(key) ? res : null;
    }

    public IndexedPeekIterator get(ByteBuffer from, ByteBuffer to) {
        return new IndexedPeekIterator(new StoragePartIterator(from, to), storagePartN);
    }

    @Override
    public void close() {
        unmap(indexBB);
        indexBB = null;
        unmap(memoryBB);
        memoryBB = null;
    }

    // Entrys count will be written in the end of index file
    public void write(Iterator<Entry<ByteBuffer>> entrysToWrite) throws IOException {
        if (entrysC != 0) {
            throw new FileAlreadyExistsException("Can't write in file that isn't empty!");
        }

        ByteBuffer bufferToWrite = ByteBuffer.allocate(DEFAULT_ALLOC_SIZE);
        int bytesWritten = 0;

        while (entrysToWrite.hasNext()) {
            Entry<ByteBuffer> entry = entrysToWrite.next();
            int entryBytesC = getPersEntryByteSize(entry);

            // Проверяем, что размеров MappedByteBuffer хватает для дальнейшей записи
            remapIfNeed(bytesWritten + entryBytesC);

            indexBB.putInt(bytesWritten);
            if (entryBytesC > bufferToWrite.capacity()) {
                bufferToWrite = ByteBuffer.allocate(entryBytesC);
            }
            persistEntry(entry, bufferToWrite);
            memoryBB.put(bufferToWrite);

            bufferToWrite.clear();
            bytesWritten += entryBytesC;

            entrysC++;
        }

        indexBB.putInt(indexBB.capacity() - Integer.BYTES, entrysC);
        indexBB.flip();
        memoryBB.flip();
        indexBB.force();
        memoryBB.force();
    }

    private void remapIfNeed(int changedSize) throws IOException {
        if (changedSize > memoryBB.capacity()) {
            memoryBB = remap(memoryBB, memoryPath, changedSize + MEMORY_BOOST_PORTION);
        }

        // "2 *" because I need memory for writing entrysC in the end
        if (indexBB.capacity() - indexBB.position() < 2 * Integer.BYTES) {
            indexBB = remap(indexBB, indexPath, indexBB.capacity() + INDEX_BOOST_PORTION);
        }
    }

    private int getGreaterOrEqual(int inLast, ByteBuffer key) {
        if (key == null) {
            return 0;
        }

        int first = 0;
        int last = inLast;
        int position = (first + last) / 2;
        Entry<ByteBuffer> curEntry = readEntry(position);

        while (!curEntry.key().equals(key) && first <= last) {
            if (curEntry.key().compareTo(key) > 0) {
                last = position - 1;
            } else {
                first = position + 1;
            }
            position = (first + last) / 2;
            curEntry = readEntry(position);
        }

        // Граничные случаи
        if (position + 1 < entrysC && curEntry.key().compareTo(key) < 0) {
            position++;
        }

        return position;
    }

    private Entry<ByteBuffer> readEntry(int entryN) {
        int ind = indexBB.getInt(entryN * Integer.BYTES);
        var key = readBytes(ind);
        if (key.isEmpty()) {
            throw new RuntimeException("Entry without key.");
        }
        ind += Integer.BYTES + key.get().length;
        var value = readBytes(ind);
        return new BaseEntry<>(ByteBuffer.wrap(key.get()), value.map(ByteBuffer::wrap).orElse(null));
    }

    private Optional<byte[]> readBytes(int ind) {
        int currInd = ind;
        int len = memoryBB.getInt(currInd);
        if (len == LEN_FOR_NULL) {
            return Optional.empty();
        }
        currInd += Integer.BYTES;
        byte[] bytes = new byte[len];
        memoryBB.get(currInd, bytes);
        return Optional.of(bytes);
    }

    /**
     * Saves entry to byteBuffer.
     *
     * @param entry         that we want to save in bufferToWrite
     * @param bufferToWrite buffer where we want to persist entry
     */
    private static void persistEntry(Entry<ByteBuffer> entry, ByteBuffer bufferToWrite) {
        bufferToWrite.putInt(entry.key().array().length);
        bufferToWrite.put(entry.key().array());

        if (entry.value() == null) {
            bufferToWrite.putInt(StoragePart.LEN_FOR_NULL);
        } else {
            bufferToWrite.putInt(entry.value().array().length);
            bufferToWrite.put(entry.value().array());
        }

        bufferToWrite.flip();
    }

    /**
     * Count byte size of entry, that we want to write in file.
     *
     * @param entry that we want to save
     * @return count of bytes
     */
    private static int getPersEntryByteSize(Entry<ByteBuffer> entry) {
        int keyLength = entry.key().array().length;
        int valueLength = entry.value() == null ? 0 : entry.value().array().length;

        return 2 * Integer.BYTES + keyLength + valueLength;
    }

    private static MappedByteBuffer remap(MappedByteBuffer oldMap, Path path, int newSize) throws IOException {
        int position = oldMap.position();
        unmap(oldMap);

        MappedByteBuffer newMap = mapFile(path, newSize);
        newMap.position(position);

        return newMap;
    }

    private static MappedByteBuffer mapFile(Path filePath) throws IOException {
        int fSize = (int) filePath.toFile().length();
        return mapFile(filePath, fSize == 0 ? MIN_MAP_SIZE : fSize);
    }

    private static MappedByteBuffer mapFile(Path filePath, int mapSize) throws IOException {
        MappedByteBuffer mappedFile;
        try (
                FileChannel fileChannel = (FileChannel) Files.newByteChannel(filePath,
                        EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE))
        ) {
            mappedFile = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, mapSize);
        }

//        System.out.println("Mapping");
        return mappedFile;
    }

    private static void unmap(MappedByteBuffer buffer) {
//        System.out.println("Unmapping");
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

    private class StoragePartIterator implements Iterator<Entry<ByteBuffer>> {
        private int nextPos;
        private final ByteBuffer to;
        private Entry<ByteBuffer> next;

        public StoragePartIterator(ByteBuffer from, ByteBuffer to) {
            this.to = to;
            nextPos = getGreaterOrEqual(entrysC - 1, from);
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
        public Entry<ByteBuffer> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            Entry<ByteBuffer> current = next;
            nextPos++;
            if (nextPos < entrysC) {
                next = readEntry(nextPos);
            }
            return current;
        }
    }
}

