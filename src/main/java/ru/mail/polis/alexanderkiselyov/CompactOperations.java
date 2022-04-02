package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

public class CompactOperations {
    private Path compactedFile;
    private Path compactedIndex;
    private final String FILE_NAME;
    private final String FILE_EXTENSION;
    private final String FILE_INDEX_NAME;
    private final String FILE_INDEX_EXTENSION;
    private final String FILE_TMP_NAME;
    private final String FILE_TMP_EXTENSION;
    private final String FILE_INDEX_TMP_NAME;
    private final String FILE_INDEX_TMP_EXTENSION;

    public CompactOperations(Map<String, String> fileNames) {
        FILE_NAME = fileNames.get("fileName");
        FILE_EXTENSION = fileNames.get("fileExtension");
        FILE_INDEX_NAME = fileNames.get("fileIndexName");
        FILE_INDEX_EXTENSION = fileNames.get("fileIndexExtension");
        FILE_TMP_NAME = fileNames.get("fileTmpName");
        FILE_TMP_EXTENSION = fileNames.get("fileTmpExtension");
        FILE_INDEX_TMP_NAME = fileNames.get("fileIndexTmpName");
        FILE_INDEX_TMP_EXTENSION = fileNames.get("fileIndexTmpExtension");
    }

    void saveDataAndIndexesCompact(Iterator<BaseEntry<byte[]>> iterator) throws IOException {
        if (!iterator.hasNext()) {
            return;
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

    void createCompactedFiles(Path basePath) throws IOException {
        compactedFile = basePath.resolve(FILE_TMP_NAME + FILE_TMP_EXTENSION);
        compactedIndex = basePath.resolve(FILE_INDEX_TMP_NAME + FILE_INDEX_TMP_EXTENSION);
        if (!Files.exists(compactedFile)) {
            Files.createFile(compactedFile);
        }
        if (!Files.exists(compactedIndex)) {
            Files.createFile(compactedIndex);
        }
    }

    void renameCompactedFile(Path basePath) throws IOException {
        if (compactedFile != null) {
            Files.move(compactedFile, basePath.resolve(FILE_NAME + "0" + FILE_EXTENSION), ATOMIC_MOVE);
        }
        if (compactedIndex != null) {
            Files.move(compactedIndex, basePath.resolve(FILE_INDEX_NAME + "0" + FILE_INDEX_EXTENSION), ATOMIC_MOVE);
        }
    }

    void clearFileIterators(List<FileIterator> fileIterators) throws IOException {
        for (FileIterator fi : fileIterators) {
            if (fi != null) {
                fi.close();
            }
        }
        fileIterators.clear();
    }

    void deleteAllFiles(List<Path> ssTables, List<Path> ssIndexes, long filesCount) throws IOException {
        for (int i = 0; i < filesCount; i++) {
            Files.delete(ssTables.get(i));
            Files.delete(ssIndexes.get(i));
        }
    }
}
