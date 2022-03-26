package ru.mail.polis.pavelkovalenko;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentNavigableMap;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

public class Serializer {

    public static Entry<ByteBuffer> readEntry(RandomAccessFile dataFile, RandomAccessFile indexesFile) throws IOException {
        int dataFileOffset = readDataFileOffset(indexesFile);
        dataFile.seek(dataFileOffset);
        byte tombstone = readByte(dataFile);
        ByteBuffer key = readByteBuffer(dataFile);
        ByteBuffer value = Utils.isTombstone(tombstone) ? null : readByteBuffer(dataFile);
        Entry<ByteBuffer> entry = new BaseEntry<>(key, value);
        dataFile.readLine();
        return entry;
    }

    public static void write(Path pathToDataFile, Path pathToIndexesFile,
                             ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> data) throws IOException {
        if (data.isEmpty()) {
            return;
        }

        try (RandomAccessFile dataFile = new RandomAccessFile(pathToDataFile.toString(), "rw");
             RandomAccessFile indexesFile = new RandomAccessFile(pathToIndexesFile.toString(), "rw")) {
            int curOffset = 0;
            int bbSize = 0;
            ByteBuffer offset = ByteBuffer.allocate(Utils.INDEX_OFFSET);
            for (Entry<ByteBuffer> entry: data.values()) {
                curOffset += bbSize;
                writeOffset(curOffset, offset, indexesFile);
                bbSize = writePair(entry, dataFile);
            }
        }
    }

    public static int sizeOf(Entry<ByteBuffer> entry) {
        int size = 1 + Integer.BYTES + entry.key().remaining();
        if (!Utils.isTombstone(entry)) {
            size += Integer.BYTES + entry.value().remaining();
        }
        size += Character.BYTES;
        return size;
    }

    private static int readDataFileOffset(RandomAccessFile indexesFile) throws IOException {
        int dataFileOffset = indexesFile.readInt();
        indexesFile.readLine();
        return dataFileOffset;
    }

    private static byte readByte(RandomAccessFile dataFile) throws IOException {
        return dataFile.readByte();
    }

    private static ByteBuffer readByteBuffer(RandomAccessFile dataFile) throws IOException {
        int bbSize = dataFile.readInt();
        ByteBuffer bb = ByteBuffer.allocate(bbSize);
        dataFile.getChannel().read(bb);
        bb.rewind();
        return bb;
    }

    /*
     * Write offsets in format:
     * ┌─────────┬────┐
     * │ integer │ \n │
     * └─────────┴────┘
     */
    private static void writeOffset(int offset, ByteBuffer bbOffset, RandomAccessFile indexesFile) throws IOException {
        bbOffset.putInt(offset);
        bbOffset.putChar(Utils.LINE_SEPARATOR);
        bbOffset.rewind();
        indexesFile.getChannel().write(bbOffset);
        bbOffset.rewind();
    }

    /*
     * Write key-value pairs in format:
     * ┌───────────────────┬────────────────────────────────────┬────────────────────────────────────────┬────┐
     * │ isTombstone: byte │ key: byte[entry.key().remaining()] │ value: byte[entry.value().remaining()] │ \n │
     * └───────────────────┴────────────────────────────────────┴────────────────────────────────────────┴────┘
     */
    private static int writePair(Entry<ByteBuffer> entry, RandomAccessFile dataFile) throws IOException {
        int bbSize = sizeOf(entry);
        ByteBuffer pair = ByteBuffer.allocate(bbSize);

        pair.put(Utils.getTombstoneValue(entry));
        pair.putInt(entry.key().remaining());
        pair.put(entry.key());
        if (!Utils.isTombstone(entry)) {
            pair.putInt(entry.value().remaining());
            pair.put(entry.value());
        }
        pair.putChar(Utils.LINE_SEPARATOR);
        pair.rewind();
        dataFile.getChannel().write(pair);

        return bbSize;
    }

}
