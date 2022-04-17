package ru.mail.polis.pavelkovalenko.iterators;

import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.Serializer;
import ru.mail.polis.pavelkovalenko.aliases.SSTable;
import ru.mail.polis.pavelkovalenko.comparators.IteratorComparator;
import ru.mail.polis.pavelkovalenko.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentNavigableMap;

public class MergeIterator implements Iterator<Entry<ByteBuffer>> {

    private final Queue<PeekIterator<Entry<ByteBuffer>>> iterators = new PriorityQueue<>(IteratorComparator.INSTANSE);
    private final TombstoneIterator tombstoneIterator;

    public MergeIterator(ByteBuffer from, ByteBuffer to, Serializer serializer, SSTable memorySSTable,
                int sstablesSize)
            throws IOException, ReflectiveOperationException {
        ByteBuffer from1 = from == null ? Utils.EMPTY_BYTEBUFFER : from;
        int priority = 0;

        if (to == null) {
            iterators.add(new PeekIterator<>(memorySSTable.tailMap(from1).values().iterator(), priority++));
        } else {
            iterators.add(new PeekIterator<>(memorySSTable.subMap(from1, to).values().iterator(), priority++));
        }

        for (; priority <= sstablesSize; ++priority) {
            iterators.add(new PeekIterator<>(
                    new FileIterator(serializer.get(sstablesSize - priority + 1), serializer, from1, to),
                    priority
            ));
        }

        this.tombstoneIterator = new TombstoneIterator(iterators);
    }

    @Override
    public boolean hasNext() {
        return tombstoneIterator.hasNext();
    }

    @Override
    public Entry<ByteBuffer> next() {
        if (iterators.isEmpty()) {
            throw new NoSuchElementException("No more elements in queue");
        }

        Entry<ByteBuffer> result = iterators.peek().peek();
        Utils.fallEntry(iterators, result);
        return result;
    }

}
