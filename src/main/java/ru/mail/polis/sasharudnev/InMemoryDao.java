package ru.mail.polis.sasharudnev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private final ConcurrentNavigableMap<String, BaseEntry<String>> data = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) {
        return (from == null ? (to == null ? data : data.headMap(to)) : (to == null ? data.tailMap(from) : data.subMap(from, to))).values().iterator();
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        data.put(entry.key(), entry);
    }
}
