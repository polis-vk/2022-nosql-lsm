package ru.mail.polis.pavelkovalenko.utils;

import ru.mail.polis.Config;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Utils {

    public static final int INDEX_OFFSET = Integer.BYTES;
    public static final ByteBuffer EMPTY_BYTEBUFFER = ByteBuffer.allocate(0);
    public static final String COMPACT_DATA_FILENAME = Utils.getDataFilename("Log");
    public static final String COMPACT_INDEXES_FILENAME = Utils.getIndexesFilename("Log");
    public static final String COMPACT_DATA_FILENAME_TO_BE_SET = "data1.txt";
    public static final String COMPACT_INDEXES_FILENAME_TO_BE_SET = "indexes1.txt";

    private static final String DATA_FILENAME = "data?.txt";
    private static final String INDEXES_FILENAME = "indexes?.txt";

    private static final Pattern DATA_FILENAME_PATTERN = Pattern.compile("data(\\d+).txt");
    private static final Pattern INDEXES_FILENAME_PATTERN = Pattern.compile("indexes(\\d+).txt");
    private static final Pattern FILE_NUMBER_PATTERN = Pattern.compile("(\\d+)");

    private Utils() {
    }

    public static boolean isDataFile(Path file) {
        return DATA_FILENAME_PATTERN.matcher(file.getFileName().toString()).matches();
    }

    public static boolean isIndexesFile(Path file) {
        return INDEXES_FILENAME_PATTERN.matcher(file.getFileName().toString()).matches();
    }

    public static boolean isCompactDataFile(Path file) {
        return file.getFileName().toString().equals(COMPACT_DATA_FILENAME);
    }

    public static boolean isCompactIndexesFile(Path file) {
        return file.getFileName().toString().equals(COMPACT_INDEXES_FILENAME);
    }

    public static Integer getFileNumber(Path file) {
        Matcher matcher = FILE_NUMBER_PATTERN.matcher(file.getFileName().toString());
        if (!matcher.find()) {
            throw new IllegalArgumentException("There is no number of filename");
        }
        return Integer.parseInt(matcher.group());
    }

    public static String getDataFilename(String suffix) {
        return DATA_FILENAME.replace("?", suffix);
    }

    public static String getIndexesFilename(String suffix) {
        return INDEXES_FILENAME.replace("?", suffix);
    }

    public static Path getFilePath(String filename, Config config) {
        return config.basePath().resolve(filename);
    }

}
