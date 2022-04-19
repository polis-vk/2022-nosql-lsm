package ru.mail.polis.pavelkovalenko.comparators;

import ru.mail.polis.pavelkovalenko.utils.FileUtils;

import java.nio.file.Path;
import java.util.Comparator;

public final class PathComparator implements Comparator<Path> {

    public static final PathComparator INSTANSE = new PathComparator();

    private PathComparator() {
    }

    @Override
    public int compare(Path p1, Path p2) {
        return FileUtils.getFileNumber(p1).compareTo(FileUtils.getFileNumber(p2));
    }

}
