package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.dto.MappedPairedFiles;
import ru.mail.polis.pavelkovalenko.utils.Utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class Serializer {

    private static final Method unmap;
    private static final Object unsafe; // 'sun.misc.Unsafe' instance

    private final AtomicInteger sstablesSize;
    private final NavigableMap<Integer, MappedPairedFiles> mappedSSTables = new TreeMap<>();
    private final Config config;

    static {
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            unmap = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
            unmap.setAccessible(true);
            Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafeField.setAccessible(true);
            unsafe = theUnsafeField.get(null);
        } catch (ReflectiveOperationException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Serializer(AtomicInteger sstablesSize, Config config) {
        this.sstablesSize = sstablesSize;
        this.config = config;
    }

    public Entry<ByteBuffer> readEntry(MappedPairedFiles mappedFilePair, int indexesPos) {
        int dataPos = readDataFileOffset(mappedFilePair.indexesFile(), indexesPos);
        byte tombstone = readByte(mappedFilePair.dataFile(), dataPos);
        ++dataPos;
        ByteBuffer key = readByteBuffer(mappedFilePair.dataFile(), dataPos);
        dataPos += (Integer.BYTES + key.remaining());
        ByteBuffer value = Utils.isTombstone(tombstone) ? null : readByteBuffer(mappedFilePair.dataFile(), dataPos);
        return new BaseEntry<>(key, value);
    }

    public void write(Iterator<Entry<ByteBuffer>> sstable, Path dataPath, Path indexesPath)
            throws IOException {
        if (!sstable.hasNext()) {
            return;
        }

        addFile(dataPath);
        addFile(indexesPath);

        try (RandomAccessFile dataFile = new RandomAccessFile(dataPath.toString(), "rw");
             RandomAccessFile indexesFile = new RandomAccessFile(indexesPath.toString(), "rw")) {
            int curOffset = (int) dataFile.getFilePointer();
            int bbSize = 0;
            ByteBuffer offset = ByteBuffer.allocate(Utils.INDEX_OFFSET);
            while (sstable.hasNext()) {
                curOffset += bbSize;
                writeOffset(curOffset, offset, indexesFile);
                bbSize = writePair(sstable.next(), dataFile);
            }
        } catch (Exception ex) {
            Files.delete(dataPath);
            Files.delete(indexesPath);
            sstablesSize.decrementAndGet();
            throw new RuntimeException(ex);
        }
    }

    public MappedPairedFiles get(int priority)
            throws IOException, ReflectiveOperationException {
        if (sstablesSize.get() != mappedSSTables.size()) {
            mapSSTables();
        }
        return mappedSSTables.get(priority);
    }

    public int sizeOf(Entry<ByteBuffer> entry) {
        entry.key().rewind();

        int size = 1 + Integer.BYTES + entry.key().remaining();
        if (!Utils.isTombstone(entry)) {
            entry.value().rewind();
            size += Integer.BYTES + entry.value().remaining();
        }
        return size;
    }

    private int readDataFileOffset(MappedByteBuffer indexesFile, int indexesPos) {
        return indexesFile.getInt(indexesPos);
    }

    private void mapSSTables()
            throws IOException, ReflectiveOperationException {
        for (MappedPairedFiles mappedPairedFile : mappedSSTables.values()) {
            unmap(mappedPairedFile.dataFile());
            unmap(mappedPairedFile.indexesFile());
        }

        for (int priority = 1; priority <= sstablesSize.get(); ++priority) {
            final String priorityStr = String.valueOf(priority);
            Path dataFile = config.basePath().resolve(Utils.getDataFilename(priorityStr));
            Path indexesFile = config.basePath().resolve(Utils.getIndexesFilename(priorityStr));

            try (FileChannel dataChannel = FileChannel.open(dataFile);
                 FileChannel indexesChannel = FileChannel.open(indexesFile)) {
                MappedByteBuffer mappedDataFile =
                        dataChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataChannel.size());
                MappedByteBuffer mappedIndexesFile =
                        indexesChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexesChannel.size());
                mappedSSTables.put(priority, new MappedPairedFiles(mappedDataFile, mappedIndexesFile));
            }
        }
    }

    private byte readByte(MappedByteBuffer dataFile, int dataPos) {
        return dataFile.get(dataPos);
    }

    private ByteBuffer readByteBuffer(MappedByteBuffer dataFile, int dataPos) {
        int bbSize = dataFile.getInt(dataPos);
        return dataFile.slice(dataPos + Integer.BYTES, bbSize);
    }

    /*
     * Write offsets in format:
     * ┌─────────┐
     * │ integer │
     * └─────────┘
     */
    private void writeOffset(int offset, ByteBuffer bbOffset, RandomAccessFile indexesFile) throws IOException {
        bbOffset.putInt(offset);
        bbOffset.rewind();
        indexesFile.getChannel().write(bbOffset);
        bbOffset.rewind();
    }

    /*
     * Write key-value pairs in format:
     *                                                      │
     * ┌───────────────────┬──────────────────┬─────────────┬───────────┬───────────────┐
     * │ isTombstone: byte │ keySize: integer │ key: byte[] │ valueSize │ value: byte[] │
     * └───────────────────┴──────────────────┴─────────────┴───────────┴───────────────┘
     *                                                      │
     */
    private int writePair(Entry<ByteBuffer> entry, RandomAccessFile dataFile) throws IOException {
        int bbSize = sizeOf(entry);
        ByteBuffer pair = ByteBuffer.allocate(bbSize);
        byte tombstone = Utils.getTombstoneValue(entry);

        pair.put(tombstone);
        pair.putInt(entry.key().remaining());
        pair.put(entry.key());

        if (!Utils.isTombstone(entry)) {
            pair.putInt(entry.value().remaining());
            pair.put(entry.value());
        }

        pair.rewind();
        dataFile.getChannel().write(pair);

        return bbSize;
    }

    private void addFile(Path file) throws IOException {
        try {
            Files.createFile(file);
        } catch (Exception ex) {
            Files.deleteIfExists(file);
            throw new RuntimeException(ex);
        }
    }

    private void unmap(MappedByteBuffer buffer) throws ReflectiveOperationException {
        unmap.invoke(unsafe, buffer);
    }

}
