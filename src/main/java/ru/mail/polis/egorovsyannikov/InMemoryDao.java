package ru.mail.polis.egorovsyannikov;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {
    ConcurrentNavigableMap<String, BaseEntry<String>> stringConcurrentSkipListMap =
            new ConcurrentSkipListMap<>(String::compareTo);

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) {
        if (from == null && to == null) {
            return getIterator(stringConcurrentSkipListMap);
        }
        if (from == null) {
            return getIterator(stringConcurrentSkipListMap.headMap(to));
        }
        if (to == null) {
            return getIterator(stringConcurrentSkipListMap.tailMap(from, true));
        }
        return getIterator(stringConcurrentSkipListMap.subMap(from, to));
    }

    @Override
    public void upsert(@NotNull BaseEntry<String> entry) {
        stringConcurrentSkipListMap.put(entry.key(), entry);
    }

    private static Iterator<BaseEntry<String>> getIterator(ConcurrentNavigableMap<String, BaseEntry<String>> map) {
        return map.values().iterator();
    }
}
