package ru.mail.polis.pavelkovalenko.visitors;

import ru.mail.polis.Config;
import ru.mail.polis.pavelkovalenko.dto.PairedFiles;
import ru.mail.polis.pavelkovalenko.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class CompactVisitor extends SimpleFileVisitor<Path> {

    private final PairedFiles compactFiles;
    private final Config config;

    public CompactVisitor(PairedFiles compactFiles, Config config) {
        this.compactFiles = compactFiles;
        this.config = config;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        if (isTargetFile(file) && !file.equals(compactFiles.dataFile()) && !file.equals(compactFiles.indexesFile())) {
            Files.delete(file);
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
        renameFile(compactFiles.dataFile(), Utils.DATA_FILENAME + 1 + Utils.FILE_EXTENSION);
        renameFile(compactFiles.indexesFile(), Utils.INDEXES_FILENAME + 1 + Utils.FILE_EXTENSION);
        return FileVisitResult.CONTINUE;
    }

    private void renameFile(Path pathToFile, String newFilename) {
        File file = new File(pathToFile.toString());
        File newFile = new File(config.basePath().resolve(newFilename).toString());
        if (!file.renameTo(newFile)) {
            throw new RuntimeException("Problems with renaming file occurred");
        }
    }

    private boolean isTargetFile(Path file) {
        return Utils.isDataFile(file) || Utils.isIndexesFile(file);
    }

}
