package ru.mail.polis.pavelkovalenko.comparators;

import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Comparator;

public final class EntryComparator implements Comparator<Entry<ByteBuffer>> {

    public static final EntryComparator INSTANSE = new EntryComparator();

    private EntryComparator() {
    }

    @Override
    public int compare(Entry<ByteBuffer> e1, Entry<ByteBuffer> e2) {
        int keyCompare = e1.key().compareTo(e2.key());
        if (keyCompare == 0 && (Utils.isTombstone(e1) || Utils.isTombstone(e2))) {
            return 0;
        }
        return keyCompare;
    }

}
