package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MergedIterator implements Iterator<BaseEntry<MemorySegment>> {
    private int emptyIterators = 0;
    private final List<Iterator<BaseEntry<MemorySegment>>> iterators;
    private final List<BaseEntry<MemorySegment>> currentEntries;
    private final List<Integer> indexesWithSameKey = new ArrayList<>();
    private final Utils utils;

    public MergedIterator(List<Iterator<BaseEntry<MemorySegment>>> iterators, Utils utils) {
        this.iterators = iterators;
        this.utils = utils;
        currentEntries = initCurrentEntries();
    }

    private List<BaseEntry<MemorySegment>> initCurrentEntries() {
        List<BaseEntry<MemorySegment>> result = new ArrayList<>();
        for (Iterator<BaseEntry<MemorySegment>> iterator : iterators) {
            if (iterator.hasNext()) {
                result.add(iterator.next());
                continue;
            }

            result.add(null);
            emptyIterators++;
        }

        return result;
    }

    @Override
    public boolean hasNext() {
        return emptyIterators < iterators.size();
    }

    @Override
    public BaseEntry<MemorySegment> next() {
        BaseEntry<MemorySegment> min = findNext();
        updateCurrentEntries();
        return min;
    }

    private BaseEntry<MemorySegment> findNext() {
        BaseEntry<MemorySegment> min = null;
        for (int i = iterators.size() - 1; i >= 0; i--) {
            BaseEntry<MemorySegment> current = currentEntries.get(i);
            if (current == null) {
                continue;
            }

            if (min == null) {
                min = current;
                indexesWithSameKey.add(i);
                continue;
            }

            int compare = utils.compareBaseEntries(current, min);
            if (compare < 0) {
                min = current;
                indexesWithSameKey.clear();
                indexesWithSameKey.add(i);
            } else if (compare == 0) {
                indexesWithSameKey.add(i);
            }
        }

        return min;
    }

    private void updateCurrentEntries() {
        for (int i : indexesWithSameKey) {
            if (iterators.get(i).hasNext()) {
                currentEntries.set(i, iterators.get(i).next());
            } else {
                emptyIterators++;
                currentEntries.set(i, null);
            }
        }
        indexesWithSameKey.clear();
    }
}
