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

public class InMemoryDao implements Dao<String, BaseEntry<String>> {
    private final ConcurrentNavigableMap<String, BaseEntry<String>> dataMap = new ConcurrentSkipListMap<>();
    private final DaoFilesManager daoFilesManager;

    public InMemoryDao(Config config) throws IOException {
        this.daoFilesManager = new DaoFilesManager(config.basePath());
    }

    public InMemoryDao() {
        daoFilesManager = null;
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        if (to != null && to.equals(from)) {
            return Collections.emptyIterator();
        }
        List<Iterator<BaseEntry<String>>> fileIterators = daoFilesManager.iterators(from, to);
        List<PeekIterator> iterators = new ArrayList<>(fileIterators.size() + 1);
        iterators.add(new PeekIterator(getDataMapIterator(from, to), 0));
        for (int i = 0; i < fileIterators.size(); i++) {
            iterators.add(new PeekIterator(fileIterators.get(i), fileIterators.size() - i));
        }
        return new MergeIterator(iterators);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        BaseEntry<String> entry = dataMap.get(key);
        if (entry != null) {
            return entry.value() == null ? null : entry;
        }
        return daoFilesManager.get(key);
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
        dataMap.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        daoFilesManager.savaData(dataMap);
        dataMap.clear();
    }

    @Override
    public void close() throws IOException {
        daoFilesManager.savaData(dataMap);
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
