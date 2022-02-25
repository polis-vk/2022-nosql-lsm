package ru.mail.polis.artyomtrofimov;

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {
    ConcurrentSkipListMap<String, BaseEntry<String>> data = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) {
        boolean isFromNull = from == null;
        boolean isToNull = to == null;
        if (isFromNull && isToNull) {
            return data.values().iterator();
        }
        if (isFromNull) {
            return data.headMap(to).values().iterator();
        }
        if (isToNull) {
            return data.tailMap(from).values().iterator();
        }
        return data.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        data.put(entry.key(), entry);
    }
}
