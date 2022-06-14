package ru.mail.polis.vladislavfetisov.iterators;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;
import ru.mail.polis.vladislavfetisov.MemorySegments;
import ru.mail.polis.vladislavfetisov.Utils;
import ru.mail.polis.vladislavfetisov.lsm.SSTable;
import ru.mail.polis.vladislavfetisov.lsm.Storage;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public final class CustomIterators {
    private CustomIterators() {

    }

    public static Iterator<Entry<MemorySegment>> merge(
            List<Iterator<Entry<MemorySegment>>> iterators) {

        return switch (iterators.size()) {
            case 0 -> Collections.emptyIterator();
            case 1 -> iterators.get(0);
            case 2 -> getMergedTwo(iterators.get(0), iterators.get(1));
            default -> mergeList(iterators);
        };
    }

    public static PeekingIterator<Entry<MemorySegment>> getMergedTwo(
            Iterator<Entry<MemorySegment>> first,
            Iterator<Entry<MemorySegment>> second) {
        return mergeTwo(new PeekingIterator<>(first), new PeekingIterator<>(second));
    }

    public static PeekingIterator<Entry<MemorySegment>> mergeList(
            List<Iterator<Entry<MemorySegment>>> iterators) {
        return iterators
                .stream()
                .map(PeekingIterator::new)
                .reduce(CustomIterators::mergeTwo)
                .orElseThrow();
    }

    /**
     * Merging two iterators.
     *
     * @param it1 first iterator
     * @param it2 second iterator, also has more priority than {@code it1}
     * @return merged iterator of {@code it1} and {@code it2}
     */
    public static PeekingIterator<Entry<MemorySegment>> mergeTwo(
            PeekingIterator<Entry<MemorySegment>> it1,
            PeekingIterator<Entry<MemorySegment>> it2) {

        return new PeekingIterator<>(new Iterator<>() {

            @Override
            public boolean hasNext() {
                return it1.hasNext() || it2.hasNext();
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                if (!it1.hasNext()) {
                    return it2.next();
                }
                if (!it2.hasNext()) {
                    return it1.next();
                }
                Entry<MemorySegment> e1 = it1.peek();
                Entry<MemorySegment> e2 = it2.peek();

                int compare = MemorySegments.compareMemorySegments(e1.key(), e2.key());
                if (compare < 0) {
                    it1.next();
                    return e1;
                } else if (compare == 0) {
                    it1.next();
                    it2.next();
                    return e2; //it2 has more priority than it1
                } else {
                    it2.next();
                    return e2;
                }
            }
        });
    }

    public static Iterator<Entry<MemorySegment>> skipTombstones(PeekingIterator<Entry<MemorySegment>> it) {

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                while (true) {
                    if (!it.hasNext()) {
                        return false;
                    }
                    Entry<MemorySegment> entry = it.peek();
                    if (!entry.isTombstone()) {
                        return true;
                    }
                    it.next();
                }
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return it.next();
            }
        };
    }

    public static PeekingIterator<Entry<MemorySegment>> getMergedIterator(
            MemorySegment from, MemorySegment to, Storage fixedStorage) {

        List<SSTable> tables = fixedStorage.ssTables();

        Iterator<Entry<MemorySegment>> memory = fixedStorage.memory().get(from, to);
        Iterator<Entry<MemorySegment>> readOnly = fixedStorage.readOnlyMemory().get(from, to);
        Iterator<Entry<MemorySegment>> disc = Utils.tablesRange(from, to, tables);

        PeekingIterator<Entry<MemorySegment>> merged = CustomIterators.getMergedTwo(readOnly, memory);
        if (!tables.isEmpty()) {
            merged = CustomIterators.getMergedTwo(disc, merged);
        }
        return merged;
    }

}
