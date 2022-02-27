package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    ConcurrentNavigableMap<ByteBuffer, ByteBuffer> data = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {

        if (from != null && to != null) {
            return new DataIterator(data.subMap(from, to).entrySet().iterator());
        }
        if (from != null) {
            return new DataIterator(data.tailMap(from).entrySet().iterator());
        }
        if (to != null) {
            return new DataIterator(data.headMap(to).entrySet().iterator());
        }
        return new DataIterator(data.entrySet().iterator());
    }

    @Override
    public BaseEntry<ByteBuffer> get(ByteBuffer key) {
        return Dao.super.get(key);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> allFrom(ByteBuffer from) {
        return Dao.super.allFrom(from);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> allTo(ByteBuffer to) {
        return Dao.super.allTo(to);
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> all() {
        return Dao.super.all();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        data.put(entry.key(), entry.value());
    }

    private static class DataIterator implements Iterator<BaseEntry<ByteBuffer>> {
        private final Iterator<Map.Entry<ByteBuffer, ByteBuffer>> dataIter;

        public DataIterator(Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator) {
            this.dataIter = iterator;
        }

        @Override
        public boolean hasNext() {
            return dataIter.hasNext();
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            Map.Entry<ByteBuffer, ByteBuffer> entry = dataIter.next();
            return new BaseEntry<>(entry.getKey(), entry.getValue());
        }
    }
}
