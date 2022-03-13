package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

class Utils {
    private static final String STORAGE_FILE_NAME = "storage";
    private static final String INDEXES_FILE_NAME = "indexes";
    private final Path basePath;

    Utils(Config config) {
        if (config != null) {
            basePath = config.basePath();
            return;
        }

        basePath = null;
    }

    public Path getStoragePath(int number) {
        return basePath.resolve(STORAGE_FILE_NAME + number);
    }

    public Path getIndexesPath(int number) {
        return basePath.resolve(INDEXES_FILE_NAME + number);
    }

    public int compareMemorySegment(MemorySegment first, MemorySegment second) {
        long firstMismatchByte = first.mismatch(second);

        if (firstMismatchByte == -1) {
            return 0;
        }

        if (firstMismatchByte == first.byteSize()) {
            return -1;
        }
        if (firstMismatchByte == second.byteSize()) {
            return 1;
        }

        byte firstByte = MemoryAccess.getByteAtOffset(first, firstMismatchByte);
        byte secondByte = MemoryAccess.getByteAtOffset(second, firstMismatchByte);

        return Byte.compare(firstByte, secondByte);
    }

    public int compareBaseEntries(BaseEntry<MemorySegment> first, BaseEntry<MemorySegment> second) {
        return compareMemorySegment(first.key(), second.key());
    }

    public void createFilesIfNotExist(int number) throws IOException {
        createFileIfNotExist(getIndexesPath(number));
        createFileIfNotExist(getStoragePath(number));
    }

    private void createFileIfNotExist(Path path) throws IOException {
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
    }
}
