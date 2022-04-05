package ru.mail.polis.pavelkovalenko.comparators;

import java.nio.file.Path;
import java.util.Comparator;

public final class PathComparator implements Comparator<Path> {

    public static final PathComparator INSTANSE = new PathComparator();

    private PathComparator() {
    }

    @Override
    public int compare(Path p1, Path p2) {
        return getFileNumber(p1).compareTo(getFileNumber(p2));
    }

    private Integer getFileNumber(Path file) {
        String filename = file.getFileName().toString();
        String[] parts = filename.split("[0-9]+");
        return Integer.parseInt(filename.replace(parts[0], "").replace(parts[1], ""));
    }

}
