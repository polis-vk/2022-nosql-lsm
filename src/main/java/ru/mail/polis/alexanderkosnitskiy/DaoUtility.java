package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.Config;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static ru.mail.polis.alexanderkosnitskiy.PersistenceDao.*;

public final class DaoUtility {
    private DaoUtility() {

    }

    public static void renameFile(Config config, String fileName, String newFileName) throws IOException {
        Path source = config.basePath().resolve(fileName);
        Files.move(source, source.resolveSibling(newFileName));
    }

    public static PersistenceDao.FilePack mapFile(Path fileName, Path indexName) throws IOException {
        try (FileChannel reader = FileChannel.open(fileName, StandardOpenOption.READ);
             FileChannel indexReader = FileChannel.open(indexName, StandardOpenOption.READ)) {
            return new PersistenceDao.FilePack(reader.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(fileName)),
                    indexReader.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexName)));
        }
    }

    public static List<PersistenceDao.FilePack> getFiles(Config config) throws IOException {
        long numberOfFiles;
        try (Stream<Path> files = Files.list(config.basePath())) {
            if (files == null) {
                numberOfFiles = 0;
            } else {
                if (Files.exists(config.basePath().resolve(INDEX + IN_PROGRESS_EXTENSION))) {
                    Files.deleteIfExists(config.basePath().resolve(FILE + COMPOSITE_EXTENSION));
                    Files.deleteIfExists(config.basePath().resolve(FILE + IN_PROGRESS_EXTENSION));
                    Files.deleteIfExists(config.basePath().resolve(INDEX + IN_PROGRESS_EXTENSION));
                }
                numberOfFiles = workWithPaths(files.toList(), config) / 2;
            }
        } catch (NoSuchFileException e) {
            numberOfFiles = 0;
        }
        List<PersistenceDao.FilePack> readers = new ArrayList<>();
        for (long i = numberOfFiles - 1; i >= 0; i--) {
            readers.add(mapFile(config.basePath().resolve(FILE + i + SAFE_EXTENSION),
                    config.basePath().resolve(INDEX + i + SAFE_EXTENSION)));
        }
        return readers;
    }

    private static long workWithPaths(List<Path> paths, Config config) throws IOException {
        long numberOfFiles = paths.size();
        for (Path path : paths) {
            if (path.toString().endsWith(INDEX + COMPOSITE_EXTENSION)) {
                deleteFiles(config, numberOfFiles);
                if (Files.exists(config.basePath().resolve(FILE + COMPOSITE_EXTENSION))) {
                    renameFile(config, FILE + COMPOSITE_EXTENSION, FILE + 0 + SAFE_EXTENSION);
                }
                renameFile(config, INDEX + COMPOSITE_EXTENSION, INDEX + 0 + SAFE_EXTENSION);
                numberOfFiles = 1;
                break;
            } else if (!path.toString().endsWith(SAFE_EXTENSION)) {
                --numberOfFiles;
            }
        }
        return numberOfFiles;
    }

    public static void deleteFiles(Config config, long amountOfFiles) throws IOException {
        for (long i = amountOfFiles - 1; i >= 0; i--) {
            Files.deleteIfExists(config.basePath().resolve(FILE + i + SAFE_EXTENSION));
            Files.deleteIfExists(config.basePath().resolve(INDEX + i + SAFE_EXTENSION));
        }
    }
}
