package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

public class CompactOperations {
    private static Path compactedFile = null;
    private static Path compactedIndex = null;
    private static final String FILE_NAME = "myData";
    private static final String FILE_EXTENSION = ".txt";
    private static final String FILE_INDEX_NAME = "myIndex";
    private static final String FILE_INDEX_EXTENSION = ".txt";
    private final Path basePath;
    private final long filesCount;

    public CompactOperations(Path basePath, long filesCount) {
        this.basePath = basePath;
        this.filesCount = filesCount;
    }

    void saveDataAndIndexesCompact(Iterator<BaseEntry<byte[]>> iterator) throws IOException {
        if (!iterator.hasNext()) {
            return;
        }
        compactedFile = basePath.resolve(FILE_NAME + filesCount + FILE_EXTENSION);
        compactedIndex = basePath.resolve(FILE_INDEX_NAME + filesCount + FILE_INDEX_EXTENSION);
        if (!Files.exists(compactedFile)) {
            Files.createFile(compactedFile);
        }
        if (!Files.exists(compactedIndex)) {
            Files.createFile(compactedIndex);
        }
        long elementsCount = 0;
        long offset = 0;
        try (RandomAccessFile rafFile = new RandomAccessFile(String.valueOf(compactedFile), "rw");
             RandomAccessFile rafIndex = new RandomAccessFile(String.valueOf(compactedIndex), "rw")) {
            writeIndexInitialPosition(rafIndex);
            while (iterator.hasNext()) {
                BaseEntry<byte[]> current = iterator.next();
                Map.Entry<byte[], BaseEntry<byte[]>> currentBaseEntry =
                        Map.entry(current.key(), new BaseEntry<>(current.key(), current.value()));
                FileOperations.writePair(rafFile, currentBaseEntry);
                offset = FileOperations.writeEntryPosition(rafIndex, currentBaseEntry, offset);
                elementsCount++;
            }
            writeIndexSize(elementsCount, rafIndex);
        }
    }

    private static void writeIndexInitialPosition(RandomAccessFile raf) throws IOException {
        ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
        raf.seek(Long.BYTES);
        longBuffer.putLong(0);
        raf.write(longBuffer.array());
    }

    private static void writeIndexSize(long elementsCount, RandomAccessFile raf) throws IOException {
        ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
        raf.seek(0);
        longBuffer.putLong(elementsCount);
        raf.write(longBuffer.array());
    }

    void renameCompactedFile() throws IOException {
        if (compactedFile != null) {
            Files.move(compactedFile, basePath.resolve(FILE_NAME + "0" + FILE_EXTENSION), ATOMIC_MOVE);
        }
        if (compactedIndex != null) {
            Files.move(compactedIndex, basePath.resolve(FILE_INDEX_NAME + "0" + FILE_INDEX_EXTENSION), ATOMIC_MOVE);
        }
    }
}
