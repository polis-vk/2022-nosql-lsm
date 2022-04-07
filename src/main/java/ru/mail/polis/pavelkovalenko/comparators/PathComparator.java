package ru.mail.polis.pavelkovalenko.comparators;

import ru.mail.polis.pavelkovalenko.utils.Utils;

import java.nio.file.Path;
import java.util.Comparator;

public final class PathComparator implements Comparator<Path> {

    public static final PathComparator INSTANSE = new PathComparator();

    private PathComparator() {
    }

    @Override
    public int compare(Path p1, Path p2) {
        return Utils.getFileNumber(p1).compareTo(Utils.getFileNumber(p2));
    }

}
