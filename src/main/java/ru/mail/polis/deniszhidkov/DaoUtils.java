package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;

public class DaoUtils {

    private static final String DATA_FILE_NAME = "storage";
    private static final String COMPACTED_QUALIFIER_NAME = "compacted_";
    private static final String FILE_EXTENSION = ".txt";
    private static final String OFFSETS_FILE_NAME = "offsets";
    private static final String TMP_QUALIFIER_NAME = "tmp_";
    private static final int COMPACTED_QUALIFIER = -1;
    private static final int TMP_QUALIFIER = -2;
    private final Path basePath;

    public DaoUtils(Path basePath) {
        this.basePath = basePath;
    }

    public Path resolvePath(String typeOfFile, int qualifier) {
        return switch (qualifier) {
            case COMPACTED_QUALIFIER -> basePath.resolve(COMPACTED_QUALIFIER_NAME + typeOfFile + FILE_EXTENSION);
            case TMP_QUALIFIER -> basePath.resolve(TMP_QUALIFIER_NAME + typeOfFile + FILE_EXTENSION);
            default -> basePath.resolve(typeOfFile + qualifier + FILE_EXTENSION);
        };
    }

    public static long getEntrySize(BaseEntry<String> entry) {
        return entry.value() == null
                ? entry.key().length() * 2L
                : (entry.key().length() + entry.value().length()) * 2L;
    }

    public void validateDAOFiles() throws IOException {
        int numberOfStorages = 0;
        int numberOfOffsets = 0;
        // Удаляем файлы из директории, не относящиеся к нашей DAO, и считаем количество storage
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(basePath)) {
            for (Path file : directoryStream) {
                String fileName = file.getFileName().toString();
                if (fileName.startsWith(DATA_FILE_NAME)) {
                    numberOfStorages++;
                } else if (fileName.startsWith(OFFSETS_FILE_NAME)) {
                    numberOfOffsets++;
                } else {
                    Files.delete(file);
                }
            }
        }
        if (numberOfStorages != numberOfOffsets) {
            throw new IllegalStateException("Number of storages and offsets didn't match");
        }
        for (int i = 0; i < numberOfStorages; i++) {
            if (!Files.exists(resolvePath(OFFSETS_FILE_NAME, i))) {
                throw new IllegalStateException("There is no offsets file for some storage: storage number " + i);
            }
        }
    }

    public CopyOnWriteArrayList<DaoReader> initDaoReaders() throws IOException {
        CopyOnWriteArrayList<DaoReader> resultList = new CopyOnWriteArrayList<>();
        // Методом validateDaoFiles() гарантируется, что существуют все файлы по порядку от 0 до N.
        for (int i = 0; ; i++) {
            try {
                resultList.add(new DaoReader(
                                resolvePath(DATA_FILE_NAME, i),
                                resolvePath(OFFSETS_FILE_NAME, i)
                        )
                );
            } catch (FileNotFoundException e) {
                break;
            }
        }
        Collections.reverse(resultList);
        return resultList;
    }

    public void closeReaders(CopyOnWriteArrayList<DaoReader> readers) throws IOException {
        for (DaoReader reader : readers) {
            reader.close();
        }
    }

    public void removeOldFiles() throws IOException {
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(basePath)) {
            for (Path file : directoryStream) {
                String fileName = file.getFileName().toString();
                if (fileName.startsWith(DATA_FILE_NAME) || fileName.startsWith(OFFSETS_FILE_NAME)) {
                    Files.delete(file);
                }
            }
        }
    }
}
