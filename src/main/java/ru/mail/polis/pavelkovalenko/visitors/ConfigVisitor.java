package ru.mail.polis.pavelkovalenko.visitors;

import ru.mail.polis.pavelkovalenko.PairedFiles;
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
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeSet;

public class ConfigVisitor extends SimpleFileVisitor<Path> {

    private final NavigableSet<Path> dataFiles = new TreeSet<>(PathComparator.INSTANSE);
    private final NavigableSet<Path> indexesFiles = new TreeSet<>(PathComparator.INSTANSE);
    private final NavigableMap<Integer, PairedFiles> sstables;
    private final Serializer serializer;

    public ConfigVisitor(NavigableMap<Integer, PairedFiles> sstables, Serializer serializer) {
        this.sstables = sstables;
        this.serializer = serializer;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
        if (Utils.isDataFile(file)) {
            dataFiles.add(file);
        } else if (Utils.isIndexesFile(file)) {
            indexesFiles.add(file);
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        if (dataFiles.size() != indexesFiles.size()) {
            throw new IllegalStateException("Mismatch in the number of data-files and indexes-files (must be equal)");
        }

        Iterator<Path> dataIterator = dataFiles.iterator();
        Iterator<Path> indexesIterator = indexesFiles.iterator();
        for (int priority = 1; priority <= dataFiles.size(); ++priority) {
            Path dataFile = dataIterator.next();
            Path indexesFile = indexesIterator.next();
            if (!isPairedFiles(dataFile, indexesFile, priority)) {
                throw new IllegalStateException("Illegal order of data- and indexes-files");
            }

            if (!serializer.hasSuccessMeta(new RandomAccessFile(dataFile.toString(), "r"))) {
                Files.delete(dataFile);
                Files.delete(indexesFile);
                continue;
            }

            sstables.put(priority, new PairedFiles(dataFile, indexesFile));
        }

        return FileVisitResult.CONTINUE;
    }

    private boolean isPairedFiles(Path dataFile, Path indexesFile, int priority) {
        int dataFileNumber = Utils.getFileNumber(dataFile);
        int indexesFileNumber = Utils.getFileNumber(indexesFile);
        return dataFileNumber == priority && dataFileNumber == indexesFileNumber;
    }

}
