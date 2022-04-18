package ru.mail.polis.pavelkovalenko.iterators;

import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.Serializer;
import ru.mail.polis.pavelkovalenko.aliases.SSTable;
import ru.mail.polis.pavelkovalenko.comparators.IteratorComparator;
import ru.mail.polis.pavelkovalenko.utils.MergeIteratorUtils;
import ru.mail.polis.pavelkovalenko.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

public class MergeIterator implements Iterator<Entry<ByteBuffer>> {

    private final Queue<PeekIterator<Entry<ByteBuffer>>> iterators = new PriorityQueue<>(IteratorComparator.INSTANSE);
    private final TombstoneIterator tombstoneIterator;

    public MergeIterator(ByteBuffer from, ByteBuffer to, Serializer serializer, List<SSTable> memorySSTables,
                         AtomicInteger sstablesSize)
            throws IOException, ReflectiveOperationException {
        ByteBuffer from1 = from == null ? Utils.EMPTY_BYTEBUFFER : from;
        int priority = 0;

        for (SSTable ssTable : memorySSTables) {
            if (to == null) {
                iterators.add(new PeekIterator<>(ssTable.tailMap(from1).values().iterator(), priority++));
            } else {
                iterators.add(new PeekIterator<>(ssTable.subMap(from1, to).values().iterator(), priority++));
            }
        }

        int snapshottedSSTablesSize = sstablesSize.get();
        for (; priority <= snapshottedSSTablesSize + memorySSTables.size() - 1; ++priority) {
            iterators.add(new PeekIterator<>(
                    new FileIterator(serializer.get(snapshottedSSTablesSize - priority + 1), serializer, from1, to),
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
        MergeIteratorUtils.fallEntry(iterators, result);
        return result;
    }

}
