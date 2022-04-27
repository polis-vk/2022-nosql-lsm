package ru.mail.polis.levsaskov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

public class StoragePart {
    public static final int LEN_FOR_NULL = -1;
    private static final int DEFAULT_ALLOC_SIZE = 2048;
    private static final int IND_BUFF_SIZE = 10;
    private final int storagePartN;
    private final MappedByteBuffer indexBB;
    private final MappedByteBuffer memoryBB;
    private int entrysC;

    private StoragePart(MappedByteBuffer indexBB, MappedByteBuffer memoryBB, int storagePartN) {
        this.storagePartN = storagePartN;
        this.memoryBB = memoryBB;
        this.indexBB = indexBB;
        // I write count of written entrys in the end of index file
        if (indexBB.capacity() != 0) {
            entrysC = indexBB.getInt(indexBB.capacity() - Integer.BYTES);
        }
    }

    public static StoragePart load(Path indexPath, Path memoryPath, int storagePartN) throws IOException {
        MappedByteBuffer indexBB = mapFile(indexPath, (int) Files.size(indexPath));
        MappedByteBuffer memoryBB = mapFile(memoryPath, (int) Files.size(memoryPath));

        return new StoragePart(indexBB, memoryBB, storagePartN);
    }

    // Entrys count will be written in the end of index file
    public static void saveSTPart(Path indexPath, Path memoryPath, Iterator<Entry<ByteBuffer>> entrysToWrite)
            throws IOException {
        ByteBuffer memBufferToWrite = ByteBuffer.allocate(DEFAULT_ALLOC_SIZE);
        ByteBuffer indBufferToWrite = ByteBuffer.allocate(Integer.BYTES * IND_BUFF_SIZE);
        int bytesWritten = 0;
        int entrysC = 0;

        try (
                FileChannel memChannel = (FileChannel) Files.newByteChannel(memoryPath,
                        EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW));
                FileChannel indChannel = (FileChannel) Files.newByteChannel(indexPath,
                        EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW));
        ) {
            while (entrysToWrite.hasNext()) {
                Entry<ByteBuffer> entry = entrysToWrite.next();
                int entryBytesC = getPersEntryByteSize(entry);

                indBufferToWrite.putInt(bytesWritten);
                if (indBufferToWrite.position() == indBufferToWrite.capacity()) {
                    indBufferToWrite.flip();
                    indChannel.write(indBufferToWrite);
                    indBufferToWrite.clear();
                }

                if (entryBytesC > memBufferToWrite.capacity()) {
                    memBufferToWrite = ByteBuffer.allocate(entryBytesC);
                }
                persistEntry(entry, memBufferToWrite);
                memChannel.write(memBufferToWrite);

                memBufferToWrite.clear();
                bytesWritten += entryBytesC;

                entrysC++;
            }
            indBufferToWrite.putInt(entrysC);
            indBufferToWrite.flip();
            indChannel.write(indBufferToWrite);
        }
    }

    public Entry<ByteBuffer> get(ByteBuffer key) {
        int position = getGreaterOrEqual(entrysC - 1, key);
        Entry<ByteBuffer> res = readEntry(position);
        return res.key().equals(key) ? res : null;
    }

    public IndexedPeekIterator get(ByteBuffer from, ByteBuffer to) {
        return new IndexedPeekIterator(new StoragePartIterator(from, to), storagePartN);
    }

    /**
     * Count byte size of entry, that we want to write in file.
     *
     * @param entry that we want to save
     * @return count of bytes
     */
    public static int getPersEntryByteSize(Entry<ByteBuffer> entry) {
        int keyLength = entry.key().array().length;
        int valueLength = entry.value() == null ? 0 : entry.value().array().length;

        return 2 * Integer.BYTES + keyLength + valueLength;
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

    private static MappedByteBuffer mapFile(Path filePath, int mapSize) throws IOException {
        MappedByteBuffer mappedFile;
        try (
                FileChannel fileChannel = (FileChannel) Files.newByteChannel(filePath,
                        EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE))
        ) {
            mappedFile = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, mapSize);
        }

        return mappedFile;
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
