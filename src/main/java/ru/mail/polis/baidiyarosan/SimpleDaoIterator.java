package ru.mail.polis.baidiyarosan;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class SimpleDaoIterator implements Iterator<BaseEntry<ByteBuffer>> {

    private final PeekIterator<BaseEntry<ByteBuffer>> iter;

    private BaseEntry<ByteBuffer> value;

    public SimpleDaoIterator(PeekIterator<BaseEntry<ByteBuffer>> iter) {
        this.iter = iter;
    }

    public BaseEntry<ByteBuffer> peek() {
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

}
