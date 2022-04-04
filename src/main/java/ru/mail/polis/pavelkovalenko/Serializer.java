package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.utils.Utils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.NavigableMap;
import java.util.TreeMap;

public final class Serializer {

    private final NavigableMap<Integer, PairedFiles> sstables;
    private final NavigableMap<Integer, MappedPairedFiles> mappedSSTables = new TreeMap<>();

    public Serializer(NavigableMap<Integer, PairedFiles> sstables) throws IOException {
        this.sstables = sstables.descendingMap();
        mapSSTables();
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

    public void write(PairedFiles pairedFiles, ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> memorySSTable)
            throws IOException {
        if (memorySSTable.isEmpty()) {
            return;
        }

        try (FileChannel dataFile = FileChannel.open(pairedFiles.dataFile(),
                StandardOpenOption.READ, StandardOpenOption.WRITE);
             FileChannel indexesFile = FileChannel.open(pairedFiles.indexesFile(),
                     StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long dataSize = 0;
            long indexesSize = (long)memorySSTable.size() * Utils.INDEX_OFFSET;
            for (Entry<ByteBuffer> entry: memorySSTable.values()) {
                dataSize += sizeOf(entry);
            }

            MappedByteBuffer mappedDataFile = Utils.mapFile(dataFile,
                    FileChannel.MapMode.READ_WRITE, dataSize);
            MappedByteBuffer mappedIndexesFile = Utils.mapFile(indexesFile,
                    FileChannel.MapMode.READ_WRITE, indexesSize);
            int curOffset = 0;
            int bbSize = 0;
            for (Entry<ByteBuffer> entry: memorySSTable.values()) {
                curOffset += bbSize;
                writeOffset(curOffset, mappedIndexesFile);
                writePair(entry, mappedDataFile);
                bbSize = sizeOf(entry);
            }
        }
    }

    public MappedPairedFiles get(int key) throws IOException {
        if (sstables.size() != mappedSSTables.size()) {
            mapSSTables();
        }
        return mappedSSTables.get(key);
    }

    public int sizeOf(Entry<ByteBuffer> entry) {
        entry.key().rewind();

        int size = 1 + Integer.BYTES + entry.key().remaining();
        if (!Utils.isTombstone(entry)) {
            entry.value().rewind();
            size += Integer.BYTES + entry.value().remaining();
        }
        size += Character.BYTES;
        return size;
    }

    private int readDataFileOffset(MappedByteBuffer indexesFile, int indexesPos) {
        return indexesFile.getInt(indexesPos);
    }

    private void mapSSTables() throws IOException {
        int priority = 1;

        for (PairedFiles filePair: sstables.values()) {
            try (FileChannel dataFile = FileChannel.open(filePair.dataFile());
                 FileChannel indexesFile = FileChannel.open(filePair.indexesFile())) {
                MappedByteBuffer mappedDataFile = Utils.mapFile(dataFile,
                        FileChannel.MapMode.READ_ONLY, dataFile.size());
                MappedByteBuffer mappedIndexesFile = Utils.mapFile(indexesFile,
                        FileChannel.MapMode.READ_ONLY, indexesFile.size());
                mappedSSTables.put(priority++, new MappedPairedFiles(mappedDataFile, mappedIndexesFile));
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
     * ┌─────────┬────┐
     * │ integer │ \n │
     * └─────────┴────┘
     */
    private void writeOffset(int offset, MappedByteBuffer indexesFile) {
        indexesFile.putInt(offset);
        indexesFile.putChar(Utils.LINE_SEPARATOR);
    }

    /*
     * Write key-value pairs in format:
     * ┌───────────────────┬────────────────────────────────────┬────────────────────────────────────────┬────┐
     * │ isTombstone: byte │ key: byte[entry.key().remaining()] │ value: byte[entry.value().remaining()] │ \n │
     * └───────────────────┴────────────────────────────────────┴────────────────────────────────────────┴────┘
     */
    private void writePair(Entry<ByteBuffer> entry, MappedByteBuffer dataFile) {
        dataFile.put(Utils.getTombstoneValue(entry));
        dataFile.putInt(entry.key().remaining());
        dataFile.put(entry.key());

        if (!Utils.isTombstone(entry)) {
            dataFile.putInt(entry.value().remaining());
            dataFile.put(entry.value());
        }

        dataFile.putChar(Utils.LINE_SEPARATOR);
    }

}
