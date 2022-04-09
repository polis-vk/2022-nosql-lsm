package ru.mail.polis.pavelkovalenko.visitors;

import ru.mail.polis.pavelkovalenko.PairedFiles;
import ru.mail.polis.pavelkovalenko.utils.Utils;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class CompactVisitor extends SimpleFileVisitor<Path> {

    private final PairedFiles compactFiles;

    public CompactVisitor(PairedFiles compactFiles) {
        this.compactFiles = compactFiles;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        if (isTargetFile(file) && !file.equals(compactFiles.dataFile()) && !file.equals(compactFiles.indexesFile())) {
            Files.delete(file);
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Path newDataFile = Path.of(Utils.DATA_FILENAME + 1 + Utils.FILE_EXTENSION);
        Path newIndexesFile = Path.of(Utils.INDEXES_FILENAME + 1 + Utils.FILE_EXTENSION);

        if (compactFiles.dataFile().getFileName().toString().equals(newDataFile.toString())) {
            return FileVisitResult.CONTINUE;
        }

        Files.move(compactFiles.dataFile(), newDataFile);
        Files.move(compactFiles.indexesFile(), newIndexesFile);
        return FileVisitResult.CONTINUE;
    }

    private boolean isTargetFile(Path file) {
        return Utils.isDataFile(file) || Utils.isIndexesFile(file);
    }

}
