package ru.mail.polis.pavelkovalenko.utils;

import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.comparators.EntryComparator;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Utils {

    public static final int INDEX_OFFSET = Integer.BYTES + Character.BYTES;
    public static final ByteBuffer EMPTY_BYTEBUFFER = ByteBuffer.allocate(0);
    public static final String DATA_FILENAME = "data";
    public static final String INDEXES_FILENAME = "indexes";
    public static final String FILE_EXTENSION = ".txt";
    public static final Byte NORMAL_VALUE = 1;
    public static final Byte TOMBSTONE_VALUE = -1;
    public static final EntryComparator entryComparator = EntryComparator.INSTANSE;

    private static final Pattern PATTERN = Pattern.compile("[a-zA-Z/.]+");
    private static final String REPLACEMENT = "";

    private Utils() {
    }

    public static boolean isTombstone(Entry<ByteBuffer> entry) {
        return entry != null && entry.value() == null;
    }

    public static boolean isTombstone(byte b) {
        return b == TOMBSTONE_VALUE;
    }

    public static byte getTombstoneValue(Entry<ByteBuffer> entry) {
        return isTombstone(entry) ? Utils.TOMBSTONE_VALUE : Utils.NORMAL_VALUE;
    }

    public static boolean isDataFile(Path file) {
        return file.getFileName().toString().startsWith(DATA_FILENAME);
    }

    public static boolean isIndexesFile(Path file) {
        return file.getFileName().toString().startsWith(INDEXES_FILENAME);
    }

    public static Integer getFileNumber(Path file) {
        Matcher matcher = PATTERN.matcher(file.getFileName().toString());
        return Integer.parseInt(matcher.replaceAll(REPLACEMENT));
    }

}
