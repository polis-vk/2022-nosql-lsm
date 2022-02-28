package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private final NavigableMap<String, String> data = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) {
        if (from == null && to == null) {
            return new DaoIterator(data.entrySet().iterator());
        }
        if (from == null) {
            return new DaoIterator(data.headMap(to).entrySet().iterator());
        }
        if (to == null) {
            return new DaoIterator(data.tailMap(from).entrySet().iterator());
        }
        return new DaoIterator(data.subMap(from, to).entrySet().iterator());
    }

    @Override
    public synchronized void upsert(BaseEntry<String> entry) {
        data.put(entry.key(), entry.value());
    }

    static class DaoIterator implements Iterator<BaseEntry<String>> {

        private final Iterator<Map.Entry<String, String>> iterator;
        private Map.Entry<String, String> tempEntry;

        DaoIterator(Iterator<Map.Entry<String, String>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public BaseEntry<String> next() {
            tempEntry = iterator.next();
            return new BaseEntry<>(tempEntry.getKey(), tempEntry.getValue());
        }
    }
}
