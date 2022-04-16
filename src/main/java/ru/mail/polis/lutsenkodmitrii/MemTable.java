package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseEntry;

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MemTable {

    private static final int NULL_BYTES = 8;
    private final long maxBytesSize;
    private final ConcurrentSkipListMap<String, BaseEntry<String>> data = new ConcurrentSkipListMap<>();
    private final AtomicInteger bytes = new AtomicInteger();
    private final AtomicBoolean isFull = new AtomicBoolean(false);
    private final AtomicBoolean onFlush = new AtomicBoolean(false);

    public MemTable(long maxBytesSize) {
        this.maxBytesSize = maxBytesSize;
    }

    public Iterator<BaseEntry<String>> iterator(String from, String to) {
        if (from == null && to == null) {
            return data.values().iterator();
        }
        if (from == null) {
            return data.headMap(to).values().iterator();
        }
        if (to == null) {
            return data.tailMap(from).values().iterator();
        }
        return data.subMap(from, to).values().iterator();
    }

    public void upsertIfFits(BaseEntry<String> entry) {
        bytes.getAndUpdate(value -> {
            int entryBytes = bytesOf(entry);
            if (value + entryBytes < maxBytesSize) {
                data.put(entry.key(), entry);
                return value + entryBytes;
            } else {
                isFull.set(true);
                return value;
            }
        });
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    public boolean isFull() {
        return isFull.get();
    }

    public AtomicBoolean onFlush() {
        return onFlush;
    }

    public int bytesOf(BaseEntry<String> entry) {
        return entry.key().length() + (entry.value() == null ? NULL_BYTES : entry.value().length());
    }
}
