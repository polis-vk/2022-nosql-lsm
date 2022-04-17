package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseEntry;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class MemStorage {

    private final List<MemTable> memTables = new CopyOnWriteArrayList<>();
    private final long tableMaxBytesSize;

    public MemStorage(long tableMaxBytesSize) {
        this.tableMaxBytesSize = tableMaxBytesSize;
        memTables.add(new MemTable(tableMaxBytesSize));
        memTables.add(new MemTable(tableMaxBytesSize));
    }

    public void upsertIfFitsFirstTable(BaseEntry<String> entry) {
        memTables.get(0).upsertIfFits(entry);
    }

    public void upsertIfFitsSecondTable(BaseEntry<String> entry) {
        memTables.get(1).upsertIfFits(entry);
    }

    public boolean firstTableFull() {
        return memTables.get(0).isFull();
    }

    public boolean secondTableIsFull() {
        return memTables.get(1).isFull();
    }

    public boolean firstTableOnFlush() {
        return memTables.get(0).onFlush().get();
    }

    public boolean secondTableOnFlush() {
        return memTables.get(1).onFlush().get();
    }

    public boolean firstTableNotOnFlushAndSetTrue() {
        return !memTables.get(0).onFlush().getAndSet(true);
    }

    public boolean secondTableNotOnFlushAndSetTrue() {
        return !memTables.get(1).onFlush().getAndSet(true);
    }

    public void clearFirstTable() {
        memTables.remove(0);
        memTables.add(new MemTable(tableMaxBytesSize));
    }

    public void clearSecondTable() {
        memTables.remove(1);
        memTables.add(new MemTable(tableMaxBytesSize));
    }

    public void clear() {
        memTables.remove(0);
        memTables.remove(1);
        memTables.add(new MemTable(tableMaxBytesSize));
        memTables.add(new MemTable(tableMaxBytesSize));
    }

    public boolean isEmpty() {
        return memTables.get(0).isEmpty() && memTables.get(1).isEmpty();
    }

    public boolean isSecondTableEmpty() {
        return memTables.get(1).isEmpty();
    }

    public void rejectUpsert() {
        throw new RuntimeException("Can`t upsert now, try later");
    }

    public Iterator<BaseEntry<String>> firstTableIterator(String from, String to) {
        return memTables.get(0).iterator(from, to);
    }

    public Iterator<BaseEntry<String>> secondTableIterator(String from, String to) {
        return memTables.get(1).iterator(from, to);
    }

    public Iterator<BaseEntry<String>> iterator(String from, String to) {
        Iterator<BaseEntry<String>> firstTableIterator = memTables.get(0).iterator(from, to);
        Iterator<BaseEntry<String>> secondTableIterator = memTables.get(1).iterator(from, to);
        if (!firstTableIterator.hasNext() && !secondTableIterator.hasNext()) {
            return Collections.emptyIterator();
        }
        if (!firstTableIterator.hasNext()) {
            return secondTableIterator(from, to);
        }
        if (!secondTableIterator.hasNext()) {
            return firstTableIterator(from, to);
        }
        return new Iterator<>() {

            private final NavigableMap<String, BaseEntry<String>> tempData = new TreeMap<>();
            private BaseEntry<String> firstTableEntry = firstTableIterator.next();
            private BaseEntry<String> secondTableEntry = secondTableIterator.next();
            private String firstTableLastReadKey = firstTableEntry.key();
            private String secondTableLastReadKey = secondTableEntry.key();

            {
                tempData.put(firstTableLastReadKey, firstTableEntry);
                tempData.put(secondTableLastReadKey, secondTableEntry);
            }

            @Override
            public boolean hasNext() {
                return !tempData.isEmpty();
            }

            @Override
            public BaseEntry<String> next() {
                BaseEntry<String> removed = tempData.pollFirstEntry().getValue();
                if (removed.key().equals(firstTableLastReadKey) && firstTableIterator.hasNext()) {
                    firstTableEntry = firstTableIterator.next();
                    tempData.put(firstTableEntry.key(), firstTableEntry);
                    firstTableLastReadKey = firstTableEntry.key();
                }
                if (removed.key().equals(secondTableLastReadKey) && secondTableIterator.hasNext()) {
                    secondTableEntry = secondTableIterator.next();
                    tempData.put(secondTableEntry.key(), secondTableEntry);
                    secondTableLastReadKey = secondTableEntry.key();
                }
                return removed;
            }
        };
    }
}
