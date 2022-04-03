package ru.mail.polis.glebkomissarov;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

public class CollapseTogether {

    private static final String INDEX = "compact";

    private CollapseTogether() {

    }

    public static void createCompact(FileWorker fw, Path dir, RangeIterator data) throws IOException {
        if (Files.notExists(dir)) {
            Files.createDirectory(dir);
        }

        List<BaseEntry<MemorySegment>> entries = new ArrayList<>();
        data.forEachRemaining(entries::add);
        fw.writeEntries(entries, dir, false, FileName.COMPACT_SAVED, FileName.COMPACT_OFFSETS);
    }

    public static void moveFile(Path source, Path distance) throws IOException {
        if (Files.notExists(distance)) {
            Files.createFile(distance);
        }

        Files.move(source, distance, ATOMIC_MOVE);
    }

    public static void removeOld(Path basePath) throws IOException {
        try (Stream<Path> str = Files.list(basePath)) {
            Path[] paths = str
                    .filter(i -> !Files.isDirectory(i) && !i.toString().contains(INDEX))
                    .toArray(Path[]::new);
            for (Path path : paths) {
                Files.delete(path);
            }
        }
    }
}
