package ru.mail.polis.baidiyarosan;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class InMemoryDaoIterator implements Iterator<BaseEntry<ByteBuffer>> {

    private final Iterator<BaseEntry<ByteBuffer>> iter;

    private BaseEntry<ByteBuffer> value;

    public InMemoryDaoIterator(Iterator<BaseEntry<ByteBuffer>> iter) {
        this.iter = iter;
    }

    @Override
    public boolean hasNext() {
        return value != null || (iter.hasNext() && peek() != null);
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        BaseEntry<ByteBuffer> peek = peek();
        value = null;
        return peek;
    }

    private BaseEntry<ByteBuffer> peek() {
        if (value == null) {
            BaseEntry<ByteBuffer> entry = iter.next();

            if (entry.value() != null) {
                value = entry;
                return value;
            }

            if (iter.hasNext()) {
                return peek();
            }
        }
        return value;
    }

}
