package ru.mail.polis.pavelkovalenko.utils;

import ru.mail.polis.Config;

import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FileUtils {

    public static final String DATA_PREFIX = "data";
    public static final String INDEXES_PREFIX = "indexes";
    public static final String EXTENSION = ".txt";
    public static final String COMPACT_PREFIX = "Log";
    public static final String COMPACT_DATA_FILENAME_TO_BE_SET = getDataFilename(1);
    public static final String COMPACT_INDEXES_FILENAME_TO_BE_SET = getIndexesFilename(1);

    private static final String DATA_FILENAME = DATA_PREFIX + "?" + EXTENSION;
    private static final String INDEXES_FILENAME = INDEXES_PREFIX + "?" + EXTENSION;
    private static final String COMPACT_DATA_FILENAME = DATA_PREFIX + COMPACT_PREFIX + "?" + EXTENSION;
    private static final String COMPACT_INDEXES_FILENAME = INDEXES_PREFIX + COMPACT_PREFIX + "?" + EXTENSION;

    private static final String NUMBER_PATTERN = "(\\d+)";
    private static final Pattern DATA_FILENAME_PATTERN = Pattern.compile(DATA_PREFIX + NUMBER_PATTERN + EXTENSION);
    private static final Pattern INDEXES_FILENAME_PATTERN = Pattern.compile(INDEXES_PREFIX + NUMBER_PATTERN + EXTENSION);
    private static final Pattern COMPACT_DATA_FILENAME_PATTERN = Pattern.compile(DATA_PREFIX + COMPACT_PREFIX + NUMBER_PATTERN + EXTENSION);
    private static final Pattern COMPACT_INDEXES_FILENAME_PATTERN = Pattern.compile(INDEXES_PREFIX + COMPACT_PREFIX + NUMBER_PATTERN + EXTENSION);
    private static final Pattern FILE_NUMBER_PATTERN = Pattern.compile(NUMBER_PATTERN);

    private FileUtils() {
    }

    public static boolean isDataFile(Path file) {
        return DATA_FILENAME_PATTERN.matcher(file.getFileName().toString()).matches();
    }

    public static boolean isIndexesFile(Path file) {
        return INDEXES_FILENAME_PATTERN.matcher(file.getFileName().toString()).matches();
    }

    public static boolean isCompactDataFile(Path file) {
        return COMPACT_DATA_FILENAME_PATTERN.matcher(file.getFileName().toString()).matches();
    }

    public static boolean isCompactIndexesFile(Path file) {
        return COMPACT_INDEXES_FILENAME_PATTERN.matcher(file.getFileName().toString()).matches();
    }

    public static Integer getFileNumber(Path file) {
        Matcher matcher = FILE_NUMBER_PATTERN.matcher(file.getFileName().toString());
        if (!matcher.find()) {
            throw new IllegalArgumentException("There is no number of filename");
        }
        return Integer.parseInt(matcher.group());
    }

    public static String getDataFilename(int ordinal) {
        return DATA_FILENAME.replace("?", String.valueOf(ordinal));
    }

    public static String getIndexesFilename(int ordinal) {
        return INDEXES_FILENAME.replace("?", String.valueOf(ordinal));
    }

    public static String getCompactDataFilename(int ordinal) {
        return COMPACT_DATA_FILENAME.replace("?", String.valueOf(ordinal));
    }

    public static String getCompactIndexesFilename(int ordinal) {
        return COMPACT_INDEXES_FILENAME.replace("?", String.valueOf(ordinal));
    }

    public static Path getFilePath(String filename, Config config) {
        return config.basePath().resolve(filename);
    }

}
