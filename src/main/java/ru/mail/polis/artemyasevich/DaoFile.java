package ru.mail.polis.artemyasevich;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class DaoFile {
    private final long[] offsets;
    private final Path pathToMeta;
    private final RandomAccessFile reader;
    private int size;
    private int entries;
    private int maxEntrySize;

    public DaoFile(Path pathToFile, Path pathToMeta) throws IOException {
        this.reader = new RandomAccessFile(pathToFile.toFile(), "r");
        this.pathToMeta = pathToMeta;
        this.offsets = processMetaAndGetOffsets();
    }

    public FileChannel getChannel() {
        return reader.getChannel();
    }

    public int entrySize(int index) {
        return (int) (offsets[index + 1] - offsets[index]);
    }

    public long sizeOfFile() {
        return size;
    }

    public int maxEntrySize() {
        return maxEntrySize;
    }

    //Returns fileSize if index == entries count
    public long getOffset(int index) {
        return offsets[index];
    }

    public void close() throws IOException {
        reader.close();
    }

    public int getLastIndex() {
        return entries - 1;
    }

    private long[] processMetaAndGetOffsets() throws IOException {
        int maxEntry = 0;
        int actualEntries;
        int actualSize = 0;
        long[] fileOffsets;
        try (DataInputStream metaStream = new DataInputStream(new BufferedInputStream(
                Files.newInputStream(pathToMeta)))) {
            //It is allowed that the size of array may be larger than necessary
            int declaredSize = metaStream.readInt();
            fileOffsets = new long[declaredSize + 1];
            long currentOffset = 0;
            fileOffsets[0] = currentOffset;
            actualEntries = 1;
            while (metaStream.available() > 0) {
                int numberOfEntries = metaStream.readInt();
                int entryBytesSize = metaStream.readInt();
                if (entryBytesSize > maxEntry) {
                    maxEntry = entryBytesSize;
                }
                for (int j = 0; j < numberOfEntries; j++) {
                    currentOffset += entryBytesSize;
                    fileOffsets[actualEntries] = currentOffset;
                    actualEntries++;
                    actualSize += entryBytesSize;
                }
            }
        }
        this.maxEntrySize = maxEntry;
        this.size = actualSize;
        this.entries = actualEntries - 1;
        return fileOffsets;
    }
}