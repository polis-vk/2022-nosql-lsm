package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class FinalIterator implements Iterator<BaseEntry<MemorySegment>> {

    private final List<Iterator<BaseEntry<MemorySegment>>> iterators;
    private final List<BaseEntry<MemorySegment>> entries = new ArrayList<>();

    public FinalIterator(List<Iterator<BaseEntry<MemorySegment>>> iterators) {
        this.iterators = iterators;

        for (int i = 0; i < iterators.size(); i++) {
            if (iterators.get(i).hasNext()) {
                entries.add(iterators.get(i).next());
            } else {
                iterators.remove(iterators.get(i));
                i--;
            }
        }
    }

    @Override
    public boolean hasNext() {
        boolean delete = false;
        MemorySegment prev = null;
        while (!entries.isEmpty()) {
            for (int i = 0; i < entries.size(); i++) {
                if (entries.get(i).value() != null
                        && (prev == null
                        || SegmentsComparator.compare(entries.get(i).key(), prev) != 0)) {
                    return true;
                }

                prev = entries.get(i).key();
                if (delete) {
                    nextOrRemove(i);
                }
            }
            delete = !delete;
        }
        return false;
    }

    @Override
    public BaseEntry<MemorySegment> next() {
        while (!entries.isEmpty()) {
            BaseEntry<MemorySegment> min = entries.get(0);
            int idx = 0;

            for (int i = 1; i < entries.size(); i++) {
                int compare = SegmentsComparator.compare(entries.get(i).key(), min.key());
                if (compare == 0) {
                    if (nextOrRemove(i)) {
                        i--;
                        continue;
                    }
                }

                if (compare < 0) {
                    idx = i;
                    min = entries.get(i);
                }
            }

            nextOrRemove(idx);
            if (min.value() != null) {
                return min;
            }
        }
        throw new NoSuchElementException();
    }

    private boolean nextOrRemove(int i) {
        if (iterators.get(i).hasNext()) {
            entries.set(i, iterators.get(i).next());
        } else {
            iterators.remove(iterators.get(i));
            entries.remove(i);
            return true;
        }
        return false;
    }
}
