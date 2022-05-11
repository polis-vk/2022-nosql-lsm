package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

class Utils {
    private static final String STORAGE_FILE_NAME = "storage";
    private static final String TMP_PREFIX = "tmp";
    private static final String COMPACTED_STORAGE_PREFIX = "compact";

    static final int TMP_FILE_NUMBER = -1;
    static final int COMPACTED_FILE_NUMBER = -2;
    private final Path basePath;

    Utils(Config config) {
        if (config != null) {
            basePath = config.basePath();
            return;
        }

        basePath = null;
    }

    public Path getStoragePath(int number) {
        if (number == TMP_FILE_NUMBER) {
            return basePath.resolve(TMP_PREFIX + STORAGE_FILE_NAME);
        }

        if (number == COMPACTED_FILE_NUMBER) {
            return basePath.resolve(COMPACTED_STORAGE_PREFIX + STORAGE_FILE_NAME);
        }

        return basePath.resolve(STORAGE_FILE_NAME + number);
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

    public void createStorageFile(int number) throws IOException {
        Files.createFile(getStoragePath(number));
    }

    public int countStorageFiles() throws IOException {
        try (Stream<Path> stream = Files.list(basePath)) {
            return (int) stream
                    .filter(this::isStorageFile)
                    .count();
        }
    }

    private boolean isStorageFile(Path path) {
        return path.getFileName().toString().startsWith(STORAGE_FILE_NAME);
    }

    public void deleteStorageFiles() throws IOException {
        try (Stream<Path> stream = Files.list(basePath)) {
            stream.filter(this::isStorageFile)
                    .forEach(this::deletePath);
        }
    }

    private void deletePath(Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public BaseEntry<MemorySegment> checkIfWasDeleted(BaseEntry<MemorySegment> entry) {
        return entry.value() == null ? null : entry;
    }
}
