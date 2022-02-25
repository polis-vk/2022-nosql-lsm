package ru.mail.polis.artyomscheredin;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

public class inMemoryDAO implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private final SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> data = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (data.isEmpty()) {
            return Collections.emptyIterator();
        }

        if ((from == null) && (to == null)) {
            return data.values().iterator();
        } else if (from == null) {
            return data.headMap(to).values().iterator();
        } else if (to == null) {
            return data.tailMap(from).values().iterator();
        }
        var t = data.subMap(from, to);
        int x = 0;
        x++;
        return t.values().iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        if (entry == null) {
            throw new IllegalArgumentException();
        }
        data.put(entry.key(), entry);
    }
}
