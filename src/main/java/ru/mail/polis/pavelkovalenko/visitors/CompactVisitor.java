package ru.mail.polis.pavelkovalenko.visitors;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import ru.mail.polis.pavelkovalenko.PairedFiles;
import ru.mail.polis.pavelkovalenko.utils.Utils;

public class CompactVisitor extends SimpleFileVisitor<Path>  {

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

    private boolean isTargetFile(Path file) {
        return Utils.isDataFile(file) || Utils.isIndexesFile(file);
    }

}
