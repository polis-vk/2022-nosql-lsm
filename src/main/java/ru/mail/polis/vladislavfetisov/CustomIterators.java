package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;

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

                int compare = Utils.compareMemorySegments(e1.key(), e2.key());
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

    public static Iterator<Entry<MemorySegment>> skipTombstones(
            LsmDao lsmDao, MemorySegment from, MemorySegment to, PeekingIterator<Entry<MemorySegment>> merged) {

        return new Iterator<>() {
            private MemorySegment prevKey = from;
            private PeekingIterator<Entry<MemorySegment>> it = merged;

            @Override
            public boolean hasNext() {
                while (true) {
                    try {
                        if (!it.hasNext()) {
                            return false;
                        }
                        Entry<MemorySegment> entry = it.peek();
                        prevKey = entry.key();
                        if (!entry.isTombstone()) {
                            return true;
                        }
                        it.next();
                    } catch (IllegalStateException e) {
                        LsmDao.logger.info("iterator is invoked again");
                        it = lsmDao.getMergedIterator(prevKey, to);
                    }
                }
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return it.next();
            }
        }

                ;
    }
}
