package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    ConcurrentNavigableMap<ByteBuffer, ByteBuffer> storage = new ConcurrentSkipListMap<>(Comparator.naturalOrder());

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return new DaoIterator(storage.entrySet().iterator());
        }
        if (from == null) {
            return new DaoIterator(storage.headMap(to, false).entrySet().iterator());
        }
        if (to == null) {
            return new DaoIterator(storage.tailMap(from, true).entrySet().iterator());
        }
        return new DaoIterator(storage.subMap(from, true, to, false).entrySet().iterator());
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        storage.put(entry.key(), entry.value());
    }

    static class DaoIterator implements Iterator<BaseEntry<ByteBuffer>> {
        private final Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator;

        private DaoIterator(Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            Map.Entry<ByteBuffer, ByteBuffer> temp = iterator.next();
            return new BaseEntry<>(temp.getKey(), temp.getValue());
        }
    }

}
