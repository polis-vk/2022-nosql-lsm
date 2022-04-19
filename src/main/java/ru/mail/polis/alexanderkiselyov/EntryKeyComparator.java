package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;

import java.util.Arrays;
import java.util.Comparator;

public final class EntryKeyComparator implements Comparator<BaseEntry<Byte[]>> {

    public static final Comparator<BaseEntry<Byte[]>> INSTANCE = new EntryKeyComparator();

    private EntryKeyComparator() {

    }

    @Override
    public int compare(BaseEntry<Byte[]> o1, BaseEntry<Byte[]> o2) {
        return Arrays.compare(o1.key(), o2.key());
    }
}
