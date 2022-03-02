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
        Map<String, BaseEntry<String>> dataSet;
        boolean isFromEqualsNull = from == null;
        boolean isToEqualsNull = to == null;

        if (isFromEqualsNull && isToEqualsNull) {
            dataSet = data;
        } else if (isFromEqualsNull) {
            dataSet = data.headMap(to);
        } else if (isToEqualsNull) {
            dataSet = data.tailMap(from);
        } else {
            dataSet = data.subMap(from, to);
        }

        return dataSet.values().iterator();
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        data.put(entry.key(), entry);
    }
}
