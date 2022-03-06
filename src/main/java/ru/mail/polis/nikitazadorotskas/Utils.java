package ru.mail.polis.nikitazadorotskas;


import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

class Utils {
    private final static String STORAGE_FILE_NAME = "storage";
    private final static String INDEXES_FILE_NAME = "sizes";
    private final Path storagePath;
    private final Path indexesPath;

    Utils(Config config) {
        if (config != null) {
            storagePath = config.basePath().resolve(STORAGE_FILE_NAME);
            indexesPath = config.basePath().resolve(INDEXES_FILE_NAME);
            return;
        }

        storagePath = null;
        indexesPath = null;
    }

    public Path getStoragePath() {
        return storagePath;
    }

    public Path getIndexesPath() {
        return indexesPath;
    }

    public static int compareMemorySegment(MemorySegment first, MemorySegment second) {
        long firstMismatchByte = first.mismatch(second);

        if (firstMismatchByte == -1) {
            return 0;
        }

        byte firstByte = MemoryAccess.getByteAtOffset(first, firstMismatchByte);
        byte secondByte = MemoryAccess.getByteAtOffset(second, firstMismatchByte);

        return Byte.compare(firstByte, secondByte);
    }

    public void createFilesIfNotExist() throws IOException {
        createFileIfNotExist(indexesPath);
        createFileIfNotExist(storagePath);
    }

    private void createFileIfNotExist(Path path) throws IOException {
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
    }
}
