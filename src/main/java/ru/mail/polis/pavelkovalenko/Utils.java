package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;
import java.nio.ByteBuffer;
import java.util.Comparator;

public final class Utils {

    public static final int INDEX_OFFSET = Integer.BYTES + Character.BYTES;
    public static final char LINE_SEPARATOR = '\n';
    public static final ByteBuffer EMPTY_BYTEBUFFER = ByteBuffer.allocate(0);
    public static final Entry<ByteBuffer> EMPTY_ENTRY = new BaseEntry<>(EMPTY_BYTEBUFFER, EMPTY_BYTEBUFFER);
    public static final String DATA_FILENAME = "data";
    public static final String INDEXES_FILENAME = "indexes";
    public static final String FILE_EXTENSION = ".txt";
    public static final EntryComparator entryComparator = new EntryComparator();
    public static final Byte NORMAL_VALUE = 1;
    public static final Byte TOMBSTONE_VALUE = -1;
    public static final Timer timer = new Timer();
    public static final IteratorComparator iteratorComparator = new IteratorComparator();

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

    public static class EntryComparator implements Comparator<Entry<ByteBuffer>> {
        @Override
        public int compare(Entry<ByteBuffer> e1, Entry<ByteBuffer> e2) {
            int keyCompare = e1.key().compareTo(e2.key());
            if (keyCompare == 0 && (Utils.isTombstone(e1) || Utils.isTombstone(e2))) {
                return 0;
            }
            return keyCompare;
        }
    }

    public static class IteratorComparator implements Comparator<PeekIterator> {
        @Override
        public int compare(PeekIterator it1, PeekIterator it2) {
            if (it1.hasNext() && it2.hasNext()) {
                int compare = Utils.entryComparator.compare(it1.peek(), it2.peek());
                if (compare == 0) {
                    compare = Integer.compare(it1.getPriority(), it2.getPriority());
                }
                return compare;
            }
            return Boolean.compare(it2.hasNext(), it1.hasNext());
        }
    }

}
