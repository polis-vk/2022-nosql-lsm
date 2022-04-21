package ru.mail.polis.dmitreemaximenko;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemTable implements Table {
    private static final Comparator<MemorySegment> COMPARATOR = NaturalOrderComparator.getInstance();
    private static final MemorySegment VERY_FIRST_KEY = MemorySegment.ofArray(new byte[]{});
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data =
            new ConcurrentSkipListMap<>(COMPARATOR);

    public void put(MemorySegment key, Entry<MemorySegment> entry) {
        data.put(key, entry);
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get() throws IOException {
        return get(null, null);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) throws IOException {
        MemorySegment fromValue = from;
        if (from == null) {
            fromValue = VERY_FIRST_KEY;
        }

        if (to == null) {
            return data.tailMap(fromValue).values().iterator();
        }
        return data.subMap(fromValue, to).values().iterator();

    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        try {
            return get();
        } catch (IOException e) {
            return Collections.emptyIterator();
        }
    }
}
