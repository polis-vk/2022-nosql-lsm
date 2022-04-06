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
    private final String fileName;
    private final String fileExtension;
    private final String fileIndexName;
    private final String fileIndexExtension;
    private final String fileTmpName;
    private final String fileTmpExtension;
    private final String fileIndexTmpName;
    private final String fileIndexTmpExtension;

    public CompactOperations(Map<String, String> fileNames) {
        fileName = fileNames.get("fileName");
        fileExtension = fileNames.get("fileExtension");
        fileIndexName = fileNames.get("fileIndexName");
        fileIndexExtension = fileNames.get("fileIndexExtension");
        fileTmpName = fileNames.get("fileTmpName");
        fileTmpExtension = fileNames.get("fileTmpExtension");
        fileIndexTmpName = fileNames.get("fileIndexTmpName");
        fileIndexTmpExtension = fileNames.get("fileIndexTmpExtension");
    }

    void saveDataAndIndexesCompact(Iterator<BaseEntry<byte[]>> iterator) throws IOException {
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
        compactedFile = basePath.resolve(fileTmpName + fileTmpExtension);
        compactedIndex = basePath.resolve(fileIndexTmpName + fileIndexTmpExtension);
        if (!Files.exists(compactedFile)) {
            Files.createFile(compactedFile);
        }
        if (!Files.exists(compactedIndex)) {
            Files.createFile(compactedIndex);
        }
    }

    void renameCompactedFile(Path basePath) throws IOException {
        if (compactedFile != null) {
            Files.move(compactedFile, basePath.resolve(fileName + "0" + fileExtension), ATOMIC_MOVE);
        }
        if (compactedIndex != null) {
            Files.move(compactedIndex, basePath.resolve(fileIndexName + "0" + fileIndexExtension), ATOMIC_MOVE);
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
