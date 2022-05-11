package ru.mail.polis.artemyasevich;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class StringDao implements Dao<String, BaseEntry<String>> {
    private final ConcurrentNavigableMap<String, BaseEntry<String>> dataMap = new ConcurrentSkipListMap<>();
    private final Storage storage;

    public StringDao(Config config) throws IOException {
        this.storage = new Storage(config);
    }

    public StringDao() {
        storage = null;
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        if (to != null && to.equals(from)) {
            return Collections.emptyIterator();
        }
        List<PeekIterator> iterators = new ArrayList<>(2);
        iterators.add(new PeekIterator(getDataMapIterator(from, to), 0));
        if (storage != null) {
            iterators.add(new PeekIterator(storage.iterate(from, to), 1));
        }
        return new MergeIterator(iterators);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        BaseEntry<String> entry = dataMap.get(key);
        if (entry != null) {
            return entry.value() == null ? null : entry;
        }
        if (storage != null) {
            entry = storage.get(key);
        }
        return entry;
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        dataMap.put(entry.key(), entry);
    }

    @Override
    public void compact() throws IOException {
        if (storage == null) {
            return;
        }
        Iterator<BaseEntry<String>> mergeIterator = get(null, null);
        if (!mergeIterator.hasNext()) {
            return;
        }
        storage.compact(mergeIterator);
        dataMap.clear();
    }

    @Override
    public void flush() throws IOException {
        if (storage == null || dataMap.isEmpty()) {
            return;
        }
        storage.flush(dataMap.values().iterator());
        dataMap.clear();
    }

    @Override
    public void close() throws IOException {
        if (storage == null) {
            return;
        }
        if (!dataMap.isEmpty()) {
            storage.flush(dataMap.values().iterator());
            dataMap.clear();
        }
        storage.closeFiles();
    }

    private Iterator<BaseEntry<String>> getDataMapIterator(String from, String to) {
        Map<String, BaseEntry<String>> subMap;
        if (from == null && to == null) {
            subMap = dataMap;
        } else if (from == null) {
            subMap = dataMap.headMap(to);
        } else if (to == null) {
            subMap = dataMap.tailMap(from);
        } else {
            subMap = dataMap.subMap(from, to);
        }
        return subMap.values().iterator();
    }

}
