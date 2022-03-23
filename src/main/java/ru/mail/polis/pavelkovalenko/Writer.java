package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.Entry;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;

public class Writer {

    private final ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> data;
    private final NavigableMap<Integer, Entry<Path>> pathsToPairedFiles;

    public Writer(ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> data,
                  NavigableMap<Integer, Entry<Path>> pathsToPairedFiles) {
        this.data = data;
        this.pathsToPairedFiles = pathsToPairedFiles;
    }

    public void write() throws IOException {
        if (data.isEmpty()) {
            return;
        }

        String pathToDataFile;
        String pathToIndexesFile;
        {
            Entry<Path> entry = pathsToPairedFiles.lastEntry().getValue();
            pathToDataFile = entry.key().toString();
            pathToIndexesFile = entry.value().toString();
        }

        try (RandomAccessFile dataFile = new RandomAccessFile(pathToDataFile,
                "rw");
             RandomAccessFile indexesFile = new RandomAccessFile(pathToIndexesFile,
                     "rw")) {
            int curOffset = 0;
            int bbSize = 0;
            ByteBuffer offset = ByteBuffer.allocate(Utils.OFFSET_VALUES_DISTANCE);
            for (Entry<ByteBuffer> entry: data.values()) {
                curOffset += bbSize;
                writeOffset(curOffset, offset, indexesFile);
                bbSize = writePair(entry, dataFile);
            }
        }
    }

    /*
     * Write offsets in format:
     * ┌──────────────┐
     * │ integer │ \n │
     * └──────────────┘
     */
    private void writeOffset(int offset, ByteBuffer bbOffset, RandomAccessFile indexesFile) throws IOException {
        bbOffset.putInt(offset);
        bbOffset.putChar(Utils.LINE_SEPARATOR);
        bbOffset.rewind();
        indexesFile.getChannel().write(bbOffset);
        bbOffset.rewind();
    }

    /*
     * Write key-value pairs in format:
     * ┌────────────────────────────────────┬─────────────────────────────────────────────┐
     * │ key: byte[entry.key().remaining()] │ value: byte[entry.value().remaining()] │ \n │
     * └────────────────────────────────────┴─────────────────────────────────────────────┘
     */
    private int writePair(Entry<ByteBuffer> entry, RandomAccessFile dataFile) throws IOException {
        int bbSize = 1 + Integer.BYTES + entry.key().remaining();
        if (!Utils.isTombstone(entry)) {
            bbSize += Integer.BYTES + entry.value().remaining();
        }
        bbSize += Character.BYTES;
        ByteBuffer pair = ByteBuffer.allocate(bbSize);

        pair.put(Utils.isTombstone(entry) ? Utils.TOMBSTONE_VALUE : Utils.NORMAL_VALUE);
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
