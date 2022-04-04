package ru.mail.polis.pavelkovalenko.comparators;

import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.iterators.PeekIterator;
import ru.mail.polis.pavelkovalenko.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Comparator;

public final class IteratorComparator implements Comparator<PeekIterator<Entry<ByteBuffer>>> {

    public static final IteratorComparator INSTANSE = new IteratorComparator();

    private IteratorComparator() {
    }

    @Override
    public int compare(PeekIterator<Entry<ByteBuffer>> it1, PeekIterator<Entry<ByteBuffer>> it2) {
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
