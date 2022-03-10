package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Author: Dmitry Kondraev.
 */

public class FileBackedDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final Comparator<MemorySegment> lexicographically = new MemorySegmentComparator();

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(lexicographically);

    private SortedStringTable sortedStringTable;
    private final Path basePath;
    private final ResourceScope dataScope = ResourceScope.newConfinedScope();

    public FileBackedDao(Config config) {
        basePath = config == null ? null : config.basePath();
    }

    public FileBackedDao() {
        this(null);
    }

    private static <K, V> Iterator<V> iterator(Map<K, V> map) {
        return map.values().iterator();
    }

    private SortedStringTable sortedStringTable() {
        if (sortedStringTable == null) {
            sortedStringTable = SortedStringTable.of(basePath, dataScope);
        }
        return sortedStringTable;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return iterator(map);
        }
        if (from == null) {
            return iterator(map.headMap(to));
        }
        if (to == null) {
            return iterator(map.tailMap(from));
        }
        return iterator(map.subMap(from, to));
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        // implicit check for non-null entry and entry.key()
        map.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> result = map.get(key);
        if (result != null) {
            return result;
        }
        if (basePath == null) {
            return null;
        }
        try {
            return sortedStringTable().get(key);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void flush() throws IOException {
        sortedStringTable()
                .write(map.values())
                .unload();
        map.clear();
    }

    @Override
    public void close() throws IOException {
        flush();
        dataScope.close();
    }
}
