package ru.mail.polis.artemyasevich;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class DaoFile {
    private final long[] offsets;
    private final RandomAccessFile reader;
    private int size;
    private int entries;
    private int maxEntrySize;

    public DaoFile(Path pathToFile, Path pathToMeta) throws IOException {
        this.reader = new RandomAccessFile(pathToFile.toFile(), "r");
        this.offsets = processMetaAndGetOffsets(pathToMeta);
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

    private long[] processMetaAndGetOffsets(Path pathToMeta) throws IOException {
        long[] fileOffsets;
        try (RandomAccessFile metaReader = new RandomAccessFile(pathToMeta.toFile(), "r")) {
            long metaFileSize = metaReader.length();
            metaReader.seek(metaFileSize - Integer.BYTES);
            this.entries = metaReader.readInt();
            fileOffsets = new long[entries + 1];
            fileOffsets[0] = 0;
            metaReader.seek(0);
            int i = 1;
            int maxEntry = 0;
            int fileSize = 0;
            long currentOffset = 0;
            while (metaReader.getFilePointer() != metaFileSize - Integer.BYTES) {
                int numberOfEntries = metaReader.readInt();
                int entryBytesSize = metaReader.readInt();
                if (entryBytesSize > maxEntry) {
                    maxEntry = entryBytesSize;
                }
                for (int j = 0; j < numberOfEntries; j++) {
                    currentOffset += entryBytesSize;
                    fileOffsets[i] = currentOffset;
                    i++;
                    fileSize += entryBytesSize;
                }
            }
            this.maxEntrySize = maxEntry;
            this.size = fileSize;
        }
        return fileOffsets;
    }
}
