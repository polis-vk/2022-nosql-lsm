package ru.mail.polis.alexanderkosnitskiy;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;

public class InMemoryDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {
    private final Set<BaseEntry<ByteBuffer>> storage =
            new ConcurrentSkipListSet<>(Comparator.comparing(BaseEntry::key));

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        if (from == null) {
            return storage.iterator();
        }
        Iterator<BaseEntry<ByteBuffer>> iter = storage.iterator();
        BaseEntry<ByteBuffer> target = iter.next();
        while (!target.key().equals(from)) {
            target = iter.next();
        }
        BaseEntry<ByteBuffer> finalTarget = target;
        return new Iterator<>() {
            private final Iterator<BaseEntry<ByteBuffer>> iterator = iter;
            private final ByteBuffer last = to;
            private BaseEntry<ByteBuffer> curr = finalTarget;

            @Override
            public boolean hasNext() {
                return (last == null || curr.key().equals(last)) && iterator.hasNext();
            }

            @Override
            public BaseEntry<ByteBuffer> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                BaseEntry<ByteBuffer> temp = curr;
                curr = iterator.next();
                return temp;
            }
        };
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        storage.add(entry);
    }

}
