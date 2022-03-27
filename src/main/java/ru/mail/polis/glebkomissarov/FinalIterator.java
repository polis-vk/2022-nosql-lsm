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

        iterators.removeIf(iterator -> !iterator.hasNext());
        for (Iterator<BaseEntry<MemorySegment>> iterator : iterators) {
            entries.add(iterator.next());
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
                        || Comparator.compare(entries.get(i).key(), prev) != 0)) {
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
            int count = 0;

            for (int i = 1; i < entries.size() + count; i++) {
                int compare = Comparator.compare(entries.get(i - count).key(), min.key());
                if (compare == 0 && nextOrRemove(i - count)) {
                    count++;
                    continue;
                }

                if (compare < 0) {
                    idx = i - count;
                    min = entries.get(i - count);
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
