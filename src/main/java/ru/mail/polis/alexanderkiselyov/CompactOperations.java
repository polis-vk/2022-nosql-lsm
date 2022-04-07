package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static ru.mail.polis.alexanderkiselyov.FileConstants.FILE_EXTENSION;
import static ru.mail.polis.alexanderkiselyov.FileConstants.FILE_INDEX_EXTENSION;
import static ru.mail.polis.alexanderkiselyov.FileConstants.FILE_INDEX_NAME;
import static ru.mail.polis.alexanderkiselyov.FileConstants.FILE_NAME;

public class CompactOperations {
    private Path compactedFile;
    private Path compactedIndex;
    private final String noSuchFile;
    private static final String FILE_START_COMPACT = "startCompact.sc";
    private static final String FILE_START_COMPACT_INDEX = "startCompactIndex.sdx";
    private static final String FILE_CONTINUE_COMPACT = "continueCompact.cc";
    private static final String FILE_CONTINUE_COMPACT_INDEX = "continueCompactIndex.cdx";

    public CompactOperations() {
        noSuchFile = "No index file associated with the data file!";
    }

    void checkFiles(Path basePath) throws IOException {
        try (Stream<Path> filesStream = Files.list(basePath)) {
            List<Path> files = filesStream.toList();
            for (Path file : files) {
                checkStartCompactConflicts(basePath, file, files);
                checkContinueCompactionConflicts(basePath, file, files);
                if (file == basePath.resolve(FILE_NAME + "0" + FILE_EXTENSION)
                        && !files.contains(basePath.resolve(FILE_INDEX_NAME + "0" + FILE_INDEX_EXTENSION))) {
                    Files.delete(basePath.resolve(FILE_NAME + "0" + FILE_EXTENSION));
                    throw new NoSuchFileException(noSuchFile);
                }
            }
        }
    }

    private void checkStartCompactConflicts(Path basePath, Path file, List<Path> files) throws IOException {
        if (file == basePath.resolve(FILE_START_COMPACT)) {
            if (files.contains(basePath.resolve(FILE_START_COMPACT_INDEX))) {
                Files.delete(basePath.resolve(FILE_START_COMPACT));
                Files.delete(basePath.resolve(FILE_START_COMPACT_INDEX));
            } else {
                Files.delete(basePath.resolve(FILE_START_COMPACT));
                throw new NoSuchFileException(noSuchFile);
            }
        }
    }

    private void checkContinueCompactionConflicts(Path basePath, Path file, List<Path> files) throws IOException {
        if (file == basePath.resolve(FILE_CONTINUE_COMPACT)) {
            if (files.contains(basePath.resolve(FILE_CONTINUE_COMPACT_INDEX))) {
                List<Path> ssTables = files
                        .stream().toList().stream()
                        .filter(f -> String.valueOf(f.getFileName()).contains(FILE_NAME))
                        .sorted(new PathsComparator(FILE_NAME, FILE_EXTENSION))
                        .collect(Collectors.toList());
                List<Path> ssIndexes = files
                        .stream().toList().stream()
                        .filter(f -> String.valueOf(f.getFileName()).contains(FILE_INDEX_NAME))
                        .sorted(new PathsComparator(FILE_INDEX_NAME, FILE_INDEX_EXTENSION))
                        .collect(Collectors.toList());
                deleteFiles(ssTables);
                deleteFiles(ssIndexes);
            } else {
                Files.delete(basePath.resolve(FILE_CONTINUE_COMPACT));
                throw new NoSuchFileException(noSuchFile);
            }
        }
    }

    private void deleteFiles(List<Path> filePaths) throws IOException {
        for (Path filePath : filePaths) {
            Files.delete(filePath);
        }
    }

    void saveDataAndIndexesCompact(Iterator<BaseEntry<byte[]>> iterator, Path basePath) throws IOException {
        long elementsCount = 0;
        long offset = 0;
        compactedFile = basePath.resolve(FILE_START_COMPACT);
        compactedIndex = basePath.resolve(FILE_START_COMPACT_INDEX);
        if (!Files.exists(compactedFile)) {
            Files.createFile(compactedFile);
        }
        if (!Files.exists(compactedIndex)) {
            Files.createFile(compactedIndex);
        }
        try (FileReaderWriter writerFile = new FileReaderWriter(compactedFile, compactedIndex)) {
            writeIndexInitialPosition(writerFile.getIndexChannel());
            while (iterator.hasNext()) {
                BaseEntry<byte[]> current = iterator.next();
                Map.Entry<byte[], BaseEntry<byte[]>> currentBaseEntry =
                        Map.entry(current.key(), new BaseEntry<>(current.key(), current.value()));
                FileOperations.writePair(writerFile.getFileChannel(), currentBaseEntry);
                offset = FileOperations.writeEntryPosition(writerFile.getIndexChannel(), currentBaseEntry, offset);
                elementsCount++;
            }
            writeIndexSize(elementsCount, writerFile.getIndexChannel());
        }
        Files.move(compactedFile, basePath.resolve(FILE_CONTINUE_COMPACT),
                ATOMIC_MOVE);
        Files.move(compactedIndex, basePath.resolve(FILE_CONTINUE_COMPACT), ATOMIC_MOVE);
        compactedFile = basePath.resolve(FILE_CONTINUE_COMPACT);
        compactedIndex = basePath.resolve(FILE_CONTINUE_COMPACT_INDEX);
    }

    private static void writeIndexInitialPosition(FileChannel channel) throws IOException {
        ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
        channel.position(Long.BYTES);
        longBuffer.putLong(0);
        longBuffer.flip();
        channel.write(longBuffer);
    }

    private static void writeIndexSize(long elementsCount, FileChannel channel) throws IOException {
        ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
        channel.position(0);
        longBuffer.putLong(elementsCount);
        longBuffer.flip();
        channel.write(longBuffer);
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

    void deleteAllFiles(List<Path> ssTables, List<Path> ssIndexes) throws IOException {
        for (Path ssTable : ssTables) {
            Files.delete(ssTable);
        }
        for (Path ssIndex : ssIndexes) {
            Files.delete(ssIndex);
        }
    }
}
