package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.utils.Utils;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.NavigableMap;
import java.util.TreeMap;

public final class Serializer {

    private final NavigableMap<Integer, PairedFiles> sstables;
    private final NavigableMap<Integer, MappedPairedFiles> mappedSSTables = new TreeMap<>();
    private final Config config;

    public Serializer(NavigableMap<Integer, PairedFiles> sstables, Config config) {
        this.sstables = sstables;
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

    public void write(Iterator<Entry<ByteBuffer>> sstable)
            throws IOException {
        if (!sstable.hasNext()) {
            return;
        }

        PairedFiles lastPairedFiles = addPairedFiles();

        try (RandomAccessFile dataFile = new RandomAccessFile(lastPairedFiles.dataFile().toString(), "rw");
             RandomAccessFile indexesFile = new RandomAccessFile(lastPairedFiles.indexesFile().toString(), "rw")) {
            int curOffset = 0;
            int bbSize = 0;
            ByteBuffer offset = ByteBuffer.allocate(Utils.INDEX_OFFSET);
            while (sstable.hasNext()) {
                curOffset += bbSize;
                writeOffset(curOffset, offset, indexesFile);
                bbSize = writePair(sstable.next(), dataFile);
            }
        }
    }

    public MappedPairedFiles get(int priority) throws IOException {
        if (sstables.size() != mappedSSTables.size()) {
            mapSSTables();
        }
        return mappedSSTables.get(mappedSSTables.size() - priority + 1);
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
    private void writeOffset(int offset, ByteBuffer bbOffset, RandomAccessFile indexesFile) throws IOException {
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

        pair.putChar(Utils.LINE_SEPARATOR);
        pair.rewind();
        dataFile.getChannel().write(pair);

        return bbSize;
    }

    private PairedFiles addPairedFiles() throws IOException {
        final int priority = sstables.size() + 1;
        Path dataFile = addFile(Utils.DATA_FILENAME, priority);
        Path indexesFile = addFile(Utils.INDEXES_FILENAME, priority);
        PairedFiles pairedFiles = new PairedFiles(dataFile, indexesFile);
        sstables.put(priority, pairedFiles);

        return pairedFiles;
    }

    private Path addFile(String filename, int priority) throws IOException {
        Path file = config.basePath().resolve(filename + priority + Utils.FILE_EXTENSION);
        createFile(file);
        return file;
    }

    private void createFile(Path file) throws IOException {
        if (!Files.exists(file)) {
            Files.createFile(file);
        }
    }

}
