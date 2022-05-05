package ru.mail.polis.dmitreemaximenko;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class MemTable implements Table {
    private static final Comparator<MemorySegment> COMPARATOR = NaturalOrderComparator.getInstance();
    private static final MemorySegment VERY_FIRST_KEY = MemorySegment.ofArray(new byte[]{});
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data =
            new ConcurrentSkipListMap<>(COMPARATOR);
    private long spaceLeft;
    private static final int SUCCESS = 0;
    private static final int FLUSH_REQUEST = -1;
    private static final int TABLE_READ_ONLY = -2;
    private AtomicBoolean flushRequested = new AtomicBoolean(false);

    public MemTable(long tableSpace) {
        this.spaceLeft = tableSpace;
    }

    public int put(MemorySegment key, Entry<MemorySegment> entry) {
        // #fix Me data race with space
        long possibleSpaceLeft = spaceLeft;
        if (data.containsKey(key) && data.get(key).value() != null) {
            possibleSpaceLeft += data.get(key).value().byteSize();
        }

        if (entry.value() == null) {
            data.put(key, entry);
            return SUCCESS;
        } else if (spaceLeft >= entry.value().byteSize()) {
            data.put(key, entry);
            spaceLeft = possibleSpaceLeft - entry.value().byteSize();
            return SUCCESS;
        }

        if (flushRequested.compareAndSet(false, true)) {
            return FLUSH_REQUEST;
        }

        return TABLE_READ_ONLY;
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
