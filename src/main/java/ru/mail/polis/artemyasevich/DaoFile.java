package ru.mail.polis.artemyasevich;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class DaoFile {
    private final long[] offsets;
    private final Path pathToMeta;
    private final Path pathToFile;
    private final RandomAccessFile reader;

    public DaoFile(Path pathToFile, Path pathToMeta) throws IOException {
        this.pathToFile = pathToFile;
        this.pathToMeta = pathToMeta;
        this.offsets = readOffsets();
        reader = new RandomAccessFile(pathToFile.toFile(), "r");
    }

    public FileChannel getChannel() {
        return reader.getChannel();
    }

    public int entrySize(int index) {
        return (int) (offsets[index + 1] - offsets[index]);
    }

    public long size() {
        return offsets[offsets.length - 1];
    }

    public int maxEntrySize() {
        long max = 0;
        for (int i = 0; i < getLastIndex() + 1; i++) {
            long size = offsets[i + 1] - offsets[i];
            if (size > max) {
                max = size;
            }
        }
        return (int) max;
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
                for (int j = 0; j < numberOfEntries; j++) {
                    currentOffset += entryBytesSize;
                    fileOffsets[i] = currentOffset;
                    i++;
                }
            }
        }
        return fileOffsets;
    }

    void fillBufferWithEntry(ByteBuffer buffer, int index) throws IOException {
        buffer.clear();
        buffer.limit(entrySize(index));
        getChannel().read(buffer, getOffset(index));
        buffer.flip();
    }

    String readKeyFromBuffer(ByteBuffer buffer) {
        short keySize = buffer.getShort();
        buffer.limit(Short.BYTES + keySize);
        return StandardCharsets.UTF_8.decode(buffer).toString();
    }

    String readValueFromBuffer(ByteBuffer buffer, int index) {
        buffer.limit(entrySize(index));
        if (!buffer.hasRemaining()) {
            return null;
        }
        short valueSize = buffer.getShort();
        if (valueSize == 0) {
            return "";
        }
        return StandardCharsets.UTF_8.decode(buffer).toString();
    }

}
