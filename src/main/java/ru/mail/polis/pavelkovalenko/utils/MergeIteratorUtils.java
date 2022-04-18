package ru.mail.polis.pavelkovalenko.utils;

import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.iterators.PeekIterator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public final class MergeIteratorUtils {

    public static final Byte NORMAL_VALUE = 1;
    public static final Byte TOMBSTONE_VALUE = -1;

    private MergeIteratorUtils() {
    }

    public static boolean isTombstone(byte b) {
        return b == TOMBSTONE_VALUE;
    }

    public static byte getTombstoneValue(Entry<ByteBuffer> entry) {
        return entry.isTombstone() ? TOMBSTONE_VALUE : NORMAL_VALUE;
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
        for (PeekIterator<Entry<ByteBuffer>> it : toBeRefreshed) {
            if (it.hasNext()) {
                iterators.add(it);
            }
        }
    }
}
