package ru.mail.polis.pavelkovalenko.comparators;

import java.nio.file.Path;
import java.util.Comparator;

public final class PathComparator implements Comparator<Path> {

    public static final PathComparator INSTANSE = new PathComparator();
    private static final String SPLIT_REGEX = "[0-9]+";
    private static final String REPLACEMENT = "";

    private PathComparator() {
    }

    @Override
    public int compare(Path p1, Path p2) {
        return getFileNumber(p1).compareTo(getFileNumber(p2));
    }

    private Integer getFileNumber(Path file) {
        String filename = file.getFileName().toString();
        String[] parts = filename.split(SPLIT_REGEX);
        return Integer.parseInt(filename.replace(parts[0], REPLACEMENT).replace(parts[1], REPLACEMENT));
    }

}
