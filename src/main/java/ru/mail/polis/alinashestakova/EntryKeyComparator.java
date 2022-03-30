package ru.mail.polis.alinashestakova;

import java.util.Comparator;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

public class EntryKeyComparator implements Comparator<BaseEntry<MemorySegment>> {

    public static final Comparator<BaseEntry<MemorySegment>> INSTANCE = new EntryKeyComparator();

    @Override
    public int compare(BaseEntry<MemorySegment> o1, BaseEntry<MemorySegment> o2) {
        return MemorySegmentComparator.INSTANCE.compare(o1.key(), o2.key());
    }
}
