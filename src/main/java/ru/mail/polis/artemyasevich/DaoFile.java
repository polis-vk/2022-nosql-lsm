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
    private int maxEntrySize;

    public DaoFile(Path pathToFile, Path pathToMeta) throws IOException {
        this.reader = new RandomAccessFile(pathToFile.toFile(), "r");
        this.pathToMeta = pathToMeta;
        this.offsets = readOffsets();
    }

    public FileChannel getChannel() {
        return reader.getChannel();
    }

    public int entrySize(int index) {
        return (int) (offsets[index + 1] - offsets[index]);
    }

    public long sizeOfFile() {
        return offsets[offsets.length - 1];
    }

    public int maxEntrySize() {
        return maxEntrySize;
    }

    //Returns fileSize if index == offsets.length - 1
    public long getOffset(int index) {
        return offsets[index];
    }

    public void close() throws IOException {
        reader.close();
    }

    public int getLastIndex() {
        return offsets.length - 2;
    }

    private long[] readOffsets() throws IOException {
        long[] fileOffsets;
        int maxEntrySize = -1;
        try (DataInputStream metaStream = new DataInputStream(new BufferedInputStream(
                Files.newInputStream(pathToMeta)))) {
            int dataSize = metaStream.readInt();
            fileOffsets = new long[dataSize + 1];

            long currentOffset = 0;
            fileOffsets[0] = currentOffset;
            int i = 1;
            while (metaStream.available() > 0) {
                int numberOfEntries = metaStream.readInt();
                int entryBytesSize = metaStream.readInt();
                if (entryBytesSize > maxEntrySize) {
                    maxEntrySize = entryBytesSize;
                }
                for (int j = 0; j < numberOfEntries; j++) {
                    currentOffset += entryBytesSize;
                    fileOffsets[i] = currentOffset;
                    i++;
                }
            }
        }
        this.maxEntrySize = maxEntrySize;
        return fileOffsets;
    }
}
