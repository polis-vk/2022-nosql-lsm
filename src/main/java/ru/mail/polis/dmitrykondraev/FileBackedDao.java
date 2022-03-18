package ru.mail.polis.dmitrykondraev;

import jdk.incubator.foreign.MemorySegment;
import org.jetbrains.annotations.NotNull;
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

    private static final Comparator<MemorySegment> LEXICOGRAPHICALLY = new MemorySegmentComparator();

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(LEXICOGRAPHICALLY);

    private SortedStringTable sortedStringTable;
    private final Path basePath;

    public FileBackedDao(Config config) {
        basePath = config == null ? null : config.basePath();
    }

    /**
     * Constructs FileBackedDao that behaves like in-memory DAO.
     */
    public FileBackedDao() {
        this(null);
    }

    private static <K, V> Iterator<V> iterator(Map<K, V> map) {
        return map.values().iterator();
    }

    private SortedStringTable sortedStringTable() {
        if (sortedStringTable == null) {
            sortedStringTable = SortedStringTable.of(basePath);
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
    public Entry<MemorySegment> get(@NotNull MemorySegment key) throws IOException {
        Entry<MemorySegment> result = map.get(key);
        if (result != null) {
            return result;
        }
        if (basePath == null) {
            // behaves like InMemoryDao if used without config
            return null;
        }
        return sortedStringTable().get(key);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        sortedStringTable()
                .write(map.values())
                .close();
        map.clear();
    }
}
