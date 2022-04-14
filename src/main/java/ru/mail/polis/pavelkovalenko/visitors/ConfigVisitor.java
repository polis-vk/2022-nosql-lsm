package ru.mail.polis.pavelkovalenko.visitors;

import ru.mail.polis.Config;
import ru.mail.polis.pavelkovalenko.Serializer;
import ru.mail.polis.pavelkovalenko.comparators.PathComparator;
import ru.mail.polis.pavelkovalenko.utils.Utils;

import java.io.IOException;
import java.io.RandomAccessFile;
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
        this.dataPathToBeSet = config.basePath().resolve(Utils.getDataFilename(Utils.COMPACTED_FILE_SUFFIX_TO_BE_SET));
        this.indexesPathToBeSet = config.basePath().resolve(Utils.getIndexesFilename(Utils.COMPACTED_FILE_SUFFIX_TO_BE_SET));
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
        if (Utils.isDataFile(file)) {
            this.dataFiles.add(file);
        } else if (Utils.isIndexesFile(file)) {
            this.indexesFiles.add(file);
        } else if (Utils.isCompactDataFile(file)) {
            this.compactedDataPath = file;
        } else if (Utils.isCompactIndexesFile(file)) {
            this.compactedIndexesPath = file;
        } else {
            throw new IllegalStateException("Config folder contains unresolved file: " + file);
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        if (this.compactedDataPath != null) {
            // It is guaranteed that this.compactedIndexesPath != null
            // We have to check only Meta's success
            if (serializer.hasSuccessMeta(new RandomAccessFile(this.compactedDataPath.toString(), "r"))) {
                for (Path dataFile : this.dataFiles) {
                    Files.delete(dataFile);
                }
                for (Path indexesFile : this.indexesFiles) {
                    Files.delete(indexesFile);
                }
                Files.move(this.compactedDataPath, this.dataPathToBeSet);
                Files.move(this.compactedIndexesPath, this.indexesPathToBeSet);
                sstablesSize.incrementAndGet();
                return FileVisitResult.CONTINUE;
            } else {
                Files.delete(this.compactedDataPath);
                Files.delete(this.compactedIndexesPath);
            }
        }

        if (this.compactedIndexesPath != null) {
            String compactedDataFilename = Utils.getDataFilename(String.valueOf(1));
            for (Path dataFile : this.dataFiles) {
                if (!dataFile.getFileName().toString().equals(compactedDataFilename)) {
                    Files.delete(dataFile);
                }
            }
            for (Path indexesFile : this.indexesFiles) {
                Files.delete(indexesFile);
            }
            // It is guaranteed that compacted data has success Meta
            Files.move(this.compactedIndexesPath, this.indexesPathToBeSet);
            sstablesSize.incrementAndGet();
            return FileVisitResult.CONTINUE;
        }

        if (dataFiles.size() != indexesFiles.size()) {
            throw new IllegalStateException("Mismatch in the number of data- and indexes-files.\n" +
                    "GOT: " + dataFiles.size() + ":" + indexesFiles.size());
        }

        Iterator<Path> dataIterator = dataFiles.iterator();
        Iterator<Path> indexesIterator = indexesFiles.iterator();
        for (int priority = 1; priority <= dataFiles.size(); ++priority) {
            Path dataFile = dataIterator.next();
            Path indexesFile = indexesIterator.next();
            if (!isPairedFiles(dataFile, indexesFile, priority)) {
                throw new IllegalStateException("Illegal order of data- and indexes-files.\n" +
                        "GOT: " + dataFile.getFileName() + " and " + indexesFile.getFileName());
            }
            sstablesSize.incrementAndGet();
        }
        return FileVisitResult.CONTINUE;
    }

    private boolean isPairedFiles(Path dataFile, Path indexesFile, int priority) {
        int dataFileNumber = Utils.getFileNumber(dataFile);
        int indexesFileNumber = Utils.getFileNumber(indexesFile);
        return dataFileNumber == priority && dataFileNumber == indexesFileNumber;
    }

}
