package ru.mail.polis.pavelkovalenko.utils;

import ru.mail.polis.Config;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.aliases.SSTable;
import ru.mail.polis.pavelkovalenko.iterators.PeekIterator;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Utils {

    public static final int INDEX_OFFSET = Integer.BYTES;
    public static final ByteBuffer EMPTY_BYTEBUFFER = ByteBuffer.allocate(0);
    public static final SSTable EMPTY_SSTABLE = new SSTable();
    public static final Byte NORMAL_VALUE = 1;
    public static final Byte TOMBSTONE_VALUE = -1;
    public static final String COMPACT_SUFFIX = "Log";
    public static final String COMPACT_DATA_FILENAME
            = Utils.getDataFilename(COMPACT_SUFFIX);
    public static final String COMPACT_INDEXES_FILENAME
            = Utils.getIndexesFilename(COMPACT_SUFFIX);
    public static final String COMPACTED_FILE_SUFFIX_TO_BE_SET = "1";

    private static final String DATA_PREFIX = "data";
    private static final String INDEXES_PREFIX = "indexes";
    private static final String REPLACEMENT_STRING = "?";
    private static final String FILE_NUMBER = "(\\d+)";
    private static final String FILE_EXTENSION = ".txt";
    private static final String DATA_FILENAME
            = DATA_PREFIX + REPLACEMENT_STRING + FILE_EXTENSION;
    private static final String INDEXES_FILENAME
            = INDEXES_PREFIX + REPLACEMENT_STRING + FILE_EXTENSION;

    private static final Pattern DATA_FILENAME_PATTERN
            = Pattern.compile(DATA_PREFIX + FILE_NUMBER + FILE_EXTENSION);
    private static final Pattern INDEXES_FILENAME_PATTERN
            = Pattern.compile(INDEXES_PREFIX + FILE_NUMBER + FILE_EXTENSION);
    private static final Pattern FILE_NUMBER_PATTERN
            = Pattern.compile(FILE_NUMBER);

    private Utils() {
    }

    public static boolean isTombstone(byte b) {
        return b == TOMBSTONE_VALUE;
    }

    public static byte getTombstoneValue(Entry<ByteBuffer> entry) {
        return entry.isTombstone() ? Utils.TOMBSTONE_VALUE : Utils.NORMAL_VALUE;
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
        return DATA_FILENAME.replace(REPLACEMENT_STRING, suffix);
    }

    public static String getIndexesFilename(String suffix) {
        return INDEXES_FILENAME.replace(REPLACEMENT_STRING, suffix);
    }

    public static Path getFilePath(String filename, Config config) {
        return config.basePath().resolve(filename);
    }

    public static void fallEntry(Queue<PeekIterator<Entry<ByteBuffer>>> iterators, Entry<ByteBuffer> toBeFallen) {
        List<PeekIterator<Entry<ByteBuffer>>> toBeRefreshed = new ArrayList<>();
        for (PeekIterator<Entry<ByteBuffer>> iterator : iterators) {
            if (iterator.hasNext() && iterator.peek().key().equals(toBeFallen.key())) {
                iterator.next();
                toBeRefreshed.add(iterator);
            }
        }

        iterators.removeAll(toBeRefreshed);
        for (PeekIterator<Entry<ByteBuffer>> it: toBeRefreshed) {
            if (it.hasNext()) {
                iterators.add(it);
            }
        }
    }

}
