package ru.mail.polis.pavelkovalenko.visitors;

import ru.mail.polis.Config;
import ru.mail.polis.pavelkovalenko.Serializer;
import ru.mail.polis.pavelkovalenko.comparators.PathComparator;
import ru.mail.polis.pavelkovalenko.utils.FileUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

public class ConfigVisitor extends SimpleFileVisitor<Path> {

    private final NavigableSet<Path> dataFiles = new TreeSet<>(PathComparator.INSTANSE);
    private final NavigableSet<Path> indexesFiles = new TreeSet<>(PathComparator.INSTANSE);
    private final Serializer serializer;
    private final AtomicInteger sstablesSize;

    private Path compactedDataPath;
    private Path compactedIndexesPath;
    private final Path dataPathToBeSet;
    private final Path indexesPathToBeSet;

    public ConfigVisitor(Config config, AtomicInteger sstablesSize, Serializer serializer) {
        this.sstablesSize = sstablesSize;
        this.serializer = serializer;
        this.dataPathToBeSet = config.basePath().resolve(FileUtils.COMPACT_DATA_FILENAME_TO_BE_SET);
        this.indexesPathToBeSet = config.basePath().resolve(FileUtils.COMPACT_INDEXES_FILENAME_TO_BE_SET);
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws FileNotFoundException {
        if (FileUtils.isDataFile(file)) {
            this.dataFiles.add(file);
        } else if (FileUtils.isIndexesFile(file)) {
            this.indexesFiles.add(file);
        } else if (FileUtils.isCompactDataFile(file)) {
            this.compactedDataPath = file;
        } else if (FileUtils.isCompactIndexesFile(file)) {
            this.compactedIndexesPath = file;
        } else {
            throw new IllegalStateException("Config folder contains unresolved file: " + file);
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        boolean isNeedToExit = finishCompact();
        if (isNeedToExit) {
            return FileVisitResult.CONTINUE;
        }

        deleteEmptyOrUnfinishedFiles(dataFiles.iterator(), indexesFiles.iterator());

        if (dataFiles.size() != indexesFiles.size()) {
            throw new IllegalStateException("Mismatch in the number of data- and indexes-files.\n"
                    + "GOT: " + dataFiles.size() + ":" + indexesFiles.size());
        }

        Iterator<Path> dataIterator = dataFiles.iterator();
        Iterator<Path> indexesIterator = indexesFiles.iterator();
        for (int priority = 1; priority <= dataFiles.size(); ++priority) {
            Path dataFile = dataIterator.next();
            Path indexesFile = indexesIterator.next();
            if (!isPairedFiles(dataFile, indexesFile, priority)) {
                throw new IllegalStateException("Illegal order of data- and indexes-files.\n"
                        + "GOT: " + dataFile.getFileName() + " and " + indexesFile.getFileName());
            }
            sstablesSize.incrementAndGet();
        }
        return FileVisitResult.CONTINUE;
    }

    private boolean finishCompact() throws IOException {
        return finishFullCompact() || finishPartialCompact();
    }

    private boolean finishFullCompact() throws IOException {
        if (this.compactedDataPath == null) {
            return false;
        }

        // It is guaranteed that this.compactedIndexesPath != null
        // We have to check only Meta's success
        if (!serializer.wasWritten(serializer.readMeta(this.compactedDataPath))) {
            Files.delete(this.compactedDataPath);
            Files.delete(this.compactedIndexesPath);
            return false;
        }

        for (Path dataFile : this.dataFiles) {
            Files.delete(dataFile);
        }
        for (Path indexesFile : this.indexesFiles) {
            Files.delete(indexesFile);
        }
        Files.move(this.compactedDataPath, this.dataPathToBeSet);
        Files.move(this.compactedIndexesPath, this.indexesPathToBeSet);
        sstablesSize.incrementAndGet();
        return true;
    }

    private boolean finishPartialCompact() throws IOException {
        if (this.compactedIndexesPath == null) {
            return false;
        }

        for (Path dataFile : this.dataFiles) {
            if (!dataFile.getFileName().toString().equals(FileUtils.COMPACT_DATA_FILENAME_TO_BE_SET)) {
                Files.delete(dataFile);
            }
        }
        for (Path indexesFile : this.indexesFiles) {
            Files.delete(indexesFile);
        }
        // It is guaranteed that compacted data has success Meta
        Files.move(this.compactedIndexesPath, this.indexesPathToBeSet);
        sstablesSize.incrementAndGet();
        return true;
    }

    private void deleteEmptyOrUnfinishedFiles(Iterator<Path> dataFiles, Iterator<Path> indexesFiles)
            throws IOException {
        while (dataFiles.hasNext()) {
            Path dataFile = dataFiles.next();
            Path indexesFile = indexesFiles.next();
            if (!serializer.wasWritten(serializer.readMeta(dataFile))) {
                Files.delete(dataFile);
                Files.delete(indexesFile);
            }
        }
    }

    private boolean isPairedFiles(Path dataFile, Path indexesFile, int priority) {
        int dataFileNumber = FileUtils.getFileNumber(dataFile);
        int indexesFileNumber = FileUtils.getFileNumber(indexesFile);
        return dataFileNumber == priority && dataFileNumber == indexesFileNumber;
    }

}
