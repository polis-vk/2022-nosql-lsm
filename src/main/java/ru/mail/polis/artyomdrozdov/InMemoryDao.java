package ru.mail.polis.artyomdrozdov;

import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, Entry<String>> {

    private final ConcurrentNavigableMap<String, Entry<String>> storage = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<Entry<String>> get(String from, String to) {
        if (from == null) {
            from = "";
        }

        if (to == null) {
            return storage.tailMap(from).values().iterator();
        }

        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<String> get(String key) {
        return storage.get(key);
    }

    @Override
    public void upsert(Entry<String> entry) {
        storage.put(entry.key(), entry);
    }
}
