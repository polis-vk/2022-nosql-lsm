package ru.mail.polis.daniilbakin;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    private final SortedMap<ByteBuffer, BaseEntry<ByteBuffer>> data = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return new DaoIterator(data.entrySet().iterator());
        }
        if (from == null) {
            return new DaoIterator(data.headMap(to).entrySet().iterator());
        }
        if (to == null) {
            return new DaoIterator(data.tailMap(from).entrySet().iterator());
        }
        return new DaoIterator(data.subMap(from, to).entrySet().iterator());
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        data.put(entry.key(), entry);
    }

    static class DaoIterator implements Iterator<BaseEntry<ByteBuffer>> {

        private final Iterator<Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>>> iterator;

        public DaoIterator(Iterator<Map.Entry<ByteBuffer, BaseEntry<ByteBuffer>>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            return iterator.next().getValue();
        }
    }

}
