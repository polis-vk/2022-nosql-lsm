package ru.mail.polis.pavelkovalenko.visitors;

import ru.mail.polis.Config;
import ru.mail.polis.pavelkovalenko.Serializer;
import ru.mail.polis.pavelkovalenko.utils.FileUtils;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicInteger;

public class CompactVisitor extends SimpleFileVisitor<Path> {

    private final Path compactedDataPath;
    private final Path compactedIndexesPath;
    private final Path dataPathToBeSet;
    private final Path indexesPathToBeSet;
    private final AtomicInteger sstablesSize;
    private final Serializer serializer;

    private int numberOfDeletedFiles;

    public CompactVisitor(Config config, Path compactedDataPath, Path compactedIndexesPath,
                          AtomicInteger sstablesSize, Serializer serializer) {
        this.compactedDataPath = compactedDataPath;
        this.compactedIndexesPath = compactedIndexesPath;
        this.dataPathToBeSet = config.basePath().resolve(FileUtils.COMPACT_DATA_FILENAME_TO_BE_SET);
        this.indexesPathToBeSet = config.basePath().resolve(FileUtils.COMPACT_INDEXES_FILENAME_TO_BE_SET);
        this.sstablesSize = sstablesSize;
        this.serializer = serializer;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        if (isTargetFile(file) && serializer.wasWritten(serializer.readMeta(file))) {
            Files.delete(file);
            ++numberOfDeletedFiles;
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        if (numberOfDeletedFiles % 2 == 1) {
            throw new IllegalStateException("Config folder was corrupted (odd number of files)");
        }
        for (; numberOfDeletedFiles > 0; numberOfDeletedFiles /= 2) {
            sstablesSize.decrementAndGet();
        }
        Files.move(this.compactedDataPath, this.dataPathToBeSet);
        Files.move(this.compactedIndexesPath, this.indexesPathToBeSet);
        return FileVisitResult.CONTINUE;
    }

    private boolean isTargetFile(Path file) {
        return FileUtils.isDataFile(file) || FileUtils.isIndexesFile(file);
    }

}
