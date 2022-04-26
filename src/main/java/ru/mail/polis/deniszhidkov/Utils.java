package ru.mail.polis.deniszhidkov;

import java.nio.file.Path;

public class Utils {

    private static final String COMPACTED_QUALIFIER_NAME = "compacted_";
    private static final String FILE_EXTENSION = ".txt";
    private static final String TMP_QUALIFIER_NAME = "tmp_";
    private static final int COMPACTED_QUALIFIER = -1;
    private static final int TMP_QUALIFIER = -2;

    public static Path resolvePath(Path directoryPath, String typeOfFile, int qualifier) {
        return switch (qualifier) {
            case COMPACTED_QUALIFIER -> directoryPath.resolve(COMPACTED_QUALIFIER_NAME + typeOfFile + FILE_EXTENSION);
            case TMP_QUALIFIER -> directoryPath.resolve(TMP_QUALIFIER_NAME + typeOfFile + FILE_EXTENSION);
            default -> directoryPath.resolve(typeOfFile + qualifier + FILE_EXTENSION);
        };
    }
}
