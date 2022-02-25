package ru.mail.polis.sasharudnev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private final ConcurrentNavigableMap<String, BaseEntry<String>> data = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) {

        int isFromAndToEqualsNull = from == null ? to == null ? 0 : 1 : -1;
        Map<String, BaseEntry<String>> dataSet;

        switch (isFromAndToEqualsNull) {
            case -1 -> dataSet = data.headMap(to);
            case 0 -> dataSet = data;
            case 1 -> dataSet = data.tailMap(to);
            default -> dataSet = data.subMap(from, to);
        }
        return dataSet.values().iterator();
    }


    @Override
    public void upsert(BaseEntry<String> entry) {
        data.put(entry.key(), entry);
    }
}