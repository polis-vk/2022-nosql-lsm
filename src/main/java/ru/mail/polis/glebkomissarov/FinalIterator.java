package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FinalIterator implements Iterator<BaseEntry<MemorySegment>> {

    private final List<Iterator<BaseEntry<MemorySegment>>> iterators;
    private final List<BaseEntry<MemorySegment>> currentEntries = new ArrayList<>();

    public FinalIterator(List<Iterator<BaseEntry<MemorySegment>>> iterators) {
        this.iterators = iterators;

        for (Iterator<BaseEntry<MemorySegment>> iterator : iterators) {
            if (iterator.hasNext()) {
                currentEntries.add(iterator.next());
            } else {
                currentEntries.add(null);
            }
        }
    }

    @Override
    public boolean hasNext() {
        for (BaseEntry<MemorySegment> currentEntry : currentEntries) {
            if (currentEntry != null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public BaseEntry<MemorySegment> next() {
        int idx = 0;
        while (currentEntries.get(idx) == null) {
            idx++;
        }
        BaseEntry<MemorySegment> min = currentEntries.get(idx);

        for (int i = idx + 1; i < currentEntries.size(); i++) {
            if (currentEntries.get(i) == null) {
                continue;
            }

            int compare = SegmentsComparator.compare(currentEntries.get(i).key(), min.key());
            if (compare == 0) {
                newOrNull(i);
            } else if (compare < 0) {
                idx = i;
                min = currentEntries.get(i);
            }
        }
        newOrNull(idx);
        return min;
    }

    private void newOrNull(int idx) {
        if (iterators.get(idx).hasNext()) {
            currentEntries.set(idx, iterators.get(idx).next());
        } else {
            currentEntries.set(idx, null);
        }
    }
}
