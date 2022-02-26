package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {
    ConcurrentNavigableMap<String, String> dataMap = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) {
        Map<String, String> subMap;
        if (from == null && to == null) {
            subMap = dataMap;
        } else if (from == null) {
            subMap = dataMap.headMap(to);
        } else if (to == null) {
            subMap = dataMap.tailMap(from);
        } else {
            subMap = dataMap.subMap(from, to);
        }
        Iterator<Map.Entry<String, String>> iterator = subMap.entrySet().iterator();

        return new Iterator<>() {
            Map.Entry<String, String> next;

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public BaseEntry<String> next() {
                next = iterator.next();
                return new BaseEntry<>(next.getKey(), next.getValue());
            }
        };
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        dataMap.put(entry.key(), entry.value());
    }
}
