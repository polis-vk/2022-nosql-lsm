package ru.mail.polis.pavelkovalenko.utils;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import ru.mail.polis.Entry;
import java.nio.ByteBuffer;
import ru.mail.polis.pavelkovalenko.comparators.EntryComparator;

public final class Utils {

    public static final int INDEX_OFFSET = Integer.BYTES + Character.BYTES;
    public static final char LINE_SEPARATOR = '\n';
    public static final ByteBuffer EMPTY_BYTEBUFFER = ByteBuffer.allocate(0);
    public static final String DATA_FILENAME = "data";
    public static final String INDEXES_FILENAME = "indexes";
    public static final String FILE_EXTENSION = ".txt";
    public static final Byte NORMAL_VALUE = 1;
    public static final Byte TOMBSTONE_VALUE = -1;
    public static final EntryComparator entryComparator = EntryComparator.INSTANSE;

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

    public static MappedByteBuffer mapFile(FileChannel channel, FileChannel.MapMode mode, long size) throws IOException {
        return channel.map(mode, 0, size);
    }

}
