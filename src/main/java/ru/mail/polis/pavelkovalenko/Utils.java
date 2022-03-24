package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Comparator;

public final class Utils {

    public static final int OFFSET_VALUES_DISTANCE = Integer.BYTES + Character.BYTES;
    public static final char LINE_SEPARATOR = '\n';
    public static final int DATA_PORTION = 100_000;
    public static final ByteBuffer EMPTY_BYTEBUFFER = ByteBuffer.allocate(0);
    public static final Entry<ByteBuffer> EMPTY_ENTRY = new BaseEntry<>(EMPTY_BYTEBUFFER, EMPTY_BYTEBUFFER);
    public static final String DATA_FILENAME = "data";
    public static final String INDEXES_FILENAME = "indexes";
    public static final String FILE_EXTENSION = ".txt";
    public static final EntryComparator entryComparator = new EntryComparator();
    public static final Byte NORMAL_VALUE = 1;
    public static final Byte TOMBSTONE_VALUE = -1;
    public static final Timer t = new Timer();

    private Utils() {
    }

    public static final class EntryComparator implements Comparator<Entry<ByteBuffer>> {
        @Override
        public int compare(Entry<ByteBuffer> o1, Entry<ByteBuffer> o2) {
            // o1 != null
            if (o2 == null || o2.key() == null) {
                return -1;
            }
            return o1.key().compareTo(o2.key());
        }
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

    public static class Timer {

        private final Clock clock = Clock.systemDefaultZone();
        private long startTime;

        public Timer() {
            this.startTime = clock.millis();
        }

        public double elapse() {
            return (double)(clock.millis() - startTime) / 1000;
        }

        public void refresh() {
            startTime = clock.millis();
        }

    }

}
