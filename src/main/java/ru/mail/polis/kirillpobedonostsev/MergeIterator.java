package ru.mail.polis.kirillpobedonostsev;

import ru.mail.polis.BaseEntry;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeSet;

public class MergeIterator implements Iterator<BaseEntry<ByteBuffer>> {

    private final Map<Iterator<BaseEntry<ByteBuffer>>, BaseEntry<ByteBuffer>> curMap;
    private final NavigableSet<Iterator<BaseEntry<ByteBuffer>>> iteratorsSet;

    public MergeIterator(List<Iterator<BaseEntry<ByteBuffer>>> iterators) {
        this.curMap = new HashMap<>();
        this.iteratorsSet = new TreeSet<>(Comparator.comparing(i -> this.curMap.get(i).key()));
        for (Iterator<BaseEntry<ByteBuffer>> iter : iterators) {
            if (iter.hasNext()) {
                curMap.put(iter, iter.next());
                this.iteratorsSet.add(iter);
            }
        }
    }

    @Override
    public boolean hasNext() {
        return !iteratorsSet.isEmpty();
    }

    @Override
    public BaseEntry<ByteBuffer> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Iterator<BaseEntry<ByteBuffer>> iter = iteratorsSet.pollFirst();
        BaseEntry<ByteBuffer> res = curMap.get(iter);
        if (iter != null && iter.hasNext()) {
            curMap.put(iter, iter.next());
            iteratorsSet.add(iter);
        }
        return res;
    }
}
