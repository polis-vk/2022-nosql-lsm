package ru.mail.polis.pavelkovalenko.comparators;

import ru.mail.polis.Entry;

import java.nio.ByteBuffer;
import java.util.Comparator;

public final class EntryComparator implements Comparator<Entry<ByteBuffer>> {

    public static final EntryComparator INSTANSE = new EntryComparator();

    private EntryComparator() {
    }

    @Override
    public int compare(Entry<ByteBuffer> e1, Entry<ByteBuffer> e2) {
        return e1.key().rewind().compareTo(e2.key().rewind());
    }

}
