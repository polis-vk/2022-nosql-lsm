package ru.mail.polis.pavelkovalenko.visitors;

import ru.mail.polis.pavelkovalenko.Serializer;
import ru.mail.polis.pavelkovalenko.comparators.PathComparator;
import ru.mail.polis.pavelkovalenko.utils.Utils;

import java.io.IOException;
import java.nio.file.FileVisitResult;
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
    private final AtomicInteger sstablesSize;

    public ConfigVisitor(AtomicInteger sstablesSize, Serializer serializer) {
        this.sstablesSize = sstablesSize;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
        if (Utils.isDataFile(file)) {
            this.dataFiles.add(file);
        } else if (Utils.isIndexesFile(file)) {
            this.indexesFiles.add(file);
        } else {
            throw new IllegalStateException("Config folder contains unresolved file: " + file);
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
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

    private boolean isPairedFiles(Path dataFile, Path indexesFile, int priority) {
        int dataFileNumber = Utils.getFileNumber(dataFile);
        int indexesFileNumber = Utils.getFileNumber(indexesFile);
        return dataFileNumber == priority && dataFileNumber == indexesFileNumber;
    }

}
