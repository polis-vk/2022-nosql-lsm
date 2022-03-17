package ru.mail.polis.artyomscheredin;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class MergeIterator implements Iterator<BaseEntry<ByteBuffer>> {

    private final List<BaseEntry<ByteBuffer>> buffer; //contains next values of all iterators, order matches iterators list
    private final List<Iterator<BaseEntry<ByteBuffer>>> iterators;

    public MergeIterator(List<Iterator<BaseEntry<ByteBuffer>>> iterators) {
        Collections.reverse(iterators);
        this.iterators = iterators;
        buffer = new ArrayList<>(iterators.size());
        for (Iterator<BaseEntry<ByteBuffer>> iterator : iterators) {
            if (iterator.hasNext()) {
                buffer.add(iterator.next());
            } else {
                buffer.add(null);
            }
        }
    }

    @Override
    public boolean hasNext() {
        for (int i = 0; i < iterators.size(); i++) {
            if (buffer.get(i) != null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        ByteBuffer minKey = findMinKeyInBuffer();
        BaseEntry<ByteBuffer> result = findNewestValueForKey(minKey);
        refreshBuffer(minKey);

        return result;
    }

    private void refreshBuffer(ByteBuffer minKey) {
        for (int i = 0; i < buffer.size(); i++) {
            if ((buffer.get(i) != null) && buffer.get(i).key().equals(minKey)) {
                Iterator<BaseEntry<ByteBuffer>> it = iterators.get(i);
                BaseEntry<ByteBuffer> entry = null;

                while (it.hasNext()) {
                    entry = it.next();
                    if (!entry.key().equals(minKey)) {
                        break;
                    }
                }
                buffer.set(i, entry);
            }
        }
    }

    private BaseEntry<ByteBuffer> findNewestValueForKey(ByteBuffer minKey) {
        BaseEntry<ByteBuffer> result = null;
        for (BaseEntry<ByteBuffer> entry : buffer) {
            if ((entry != null) && entry.key().equals(minKey)) {
                result = entry;
                break;
            }
        }
        return result;
    }

    private ByteBuffer findMinKeyInBuffer() {
        ByteBuffer minKey = null;
        for (BaseEntry<ByteBuffer> entry : buffer) {
            if ((entry != null) && ((minKey == null) || (entry.key().compareTo(minKey) < 0))) {
                minKey = entry.key();
            }
        }
        return minKey;
    }
}
