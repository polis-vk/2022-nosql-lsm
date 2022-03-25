package ru.mail.polis.artemyasevich;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DaoFile {
    private final long[] offsets;
    private final Path pathToMeta;
    private final Path pathToFile;

    public DaoFile(Path pathToFile, Path pathToMeta) throws IOException {
        this.pathToFile = pathToFile;
        this.pathToMeta = pathToMeta;
        this.offsets = readOffsets();
    }

    //Returns fileSize if index == offsets.length - 1
    public long getOffset(int index) {
        return offsets[index];
    }

    public Path getPathToFile() {
        return pathToFile;
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
}
