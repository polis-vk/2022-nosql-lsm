package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.Config;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class DaoUtility {
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
}
