package ru.mail.polis.dmitreemaximenko;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {
    private final NavigableSet<BaseEntry<byte[]>> data =
            new ConcurrentSkipListSet<>(new RecordNaturalOrderComparator());

    @Override
    public BaseEntry<byte[]> get(byte @NotNull [] key) {
        Iterator<BaseEntry<byte[]>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }
        BaseEntry<byte[]> next = iterator.next();
        if (Arrays.equals(next.key(), key)) {
            return next;
        }
        return null;
    }

    @Override
    public Iterator<BaseEntry<byte[]>> get(byte[] from, byte[] to) {
        if (from == null) {
            return new BorderedIterator(data.iterator(), to);
        }
        return new BorderedIterator(data.tailSet(new BaseEntry<>(from, null), true).iterator(), to);
    }

    @Override
    public void upsert(BaseEntry<byte[]> entry) {
        data.remove(entry);
        data.add(entry);
    }

    static class BorderedIterator implements Iterator<BaseEntry<byte[]>> {
        private final Iterator<BaseEntry<byte[]>> iterator;
        private final byte[] last;
        private BaseEntry<byte[]> next;

        private BorderedIterator(Iterator<BaseEntry<byte[]>> iterator, byte[] last) {
            this.iterator = iterator;
            next = iterator.hasNext() ? iterator.next() : null;
            this.last = last == null ? null : Arrays.copyOf(last, last.length);
        }

        @Override
        public boolean hasNext() {
            return next != null && !Arrays.equals(next.key(), last);
        }

        @Override
        public BaseEntry<byte[]> next() {
            BaseEntry<byte[]> temp = next;
            next = iterator.hasNext() ? iterator.next() : null;
            return temp;
        }
    }

    static class RecordNaturalOrderComparator implements Comparator<BaseEntry<byte[]>> {
        @Override
        public int compare(BaseEntry<byte[]> e1, BaseEntry<byte[]> e2) {
            byte[] key1 = e1.key();
            byte[] key2 = e2.key();
            for (int i = 0; i < Math.min(key1.length, key2.length); ++i) {
                if (key1[i] != key2[i]) {
                    return key1[i] - key2[i];
                }
            }
            return key1.length - key2.length;
        }
    }
}
