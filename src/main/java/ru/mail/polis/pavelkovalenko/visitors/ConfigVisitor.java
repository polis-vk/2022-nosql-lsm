package ru.mail.polis.pavelkovalenko.visitors;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeSet;
import ru.mail.polis.pavelkovalenko.PairedFiles;
import ru.mail.polis.pavelkovalenko.comparators.PathComparator;
import ru.mail.polis.pavelkovalenko.utils.Utils;

public class ConfigVisitor extends SimpleFileVisitor<Path> {

    private final NavigableSet<Path> dataFiles = new TreeSet<>(PathComparator.INSTANSE);
    private final NavigableSet<Path> indexesFiles = new TreeSet<>(PathComparator.INSTANSE);
    private final NavigableMap<Integer, PairedFiles> sstables;

    public ConfigVisitor(NavigableMap<Integer, PairedFiles> sstables) {
        this.sstables = sstables;
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
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
        if (dataFiles.size() != indexesFiles.size()) {
            return FileVisitResult.TERMINATE;
        }

        Iterator<Path> dataIterator = dataFiles.iterator();
        Iterator<Path> indexesIterator = indexesFiles.iterator();
        for (int priority = 1; priority <= dataFiles.size(); ++priority) {
            sstables.put(priority, new PairedFiles(dataIterator.next(), indexesIterator.next()));
        }

        return FileVisitResult.CONTINUE;
    }

}
