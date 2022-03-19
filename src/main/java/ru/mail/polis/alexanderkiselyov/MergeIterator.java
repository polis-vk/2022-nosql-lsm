package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;

import java.util.Arrays;
import java.util.Iterator;

public class MergeIterator implements Iterator<BaseEntry<byte[]>> {
    private final Iterator<BaseEntry<byte[]>> memoryIterator;
    private final Iterator<BaseEntry<byte[]>> diskIterator;
    private BaseEntry<byte[]> currentMemoryEntry;
    private BaseEntry<byte[]> currentDiskEntry;

    public MergeIterator(Iterator<BaseEntry<byte[]>> memoryIterator, Iterator<BaseEntry<byte[]>> diskIterator) {
        this.memoryIterator = memoryIterator;
        this.diskIterator = diskIterator;
        currentMemoryEntry = memoryIterator.hasNext() ? memoryIterator.next() : null;
        currentDiskEntry = diskIterator.hasNext() ? diskIterator.next() : null;
    }

    @Override
    public boolean hasNext() {
        if (currentMemoryEntry != null && currentMemoryEntry.value() == null && !memoryIterator.hasNext()) {
            return false;
        }
        if (currentDiskEntry != null && currentDiskEntry.value() == null && !diskIterator.hasNext()) {
            return false;
        }
        skipNullValues();
        return currentMemoryEntry != null
                || currentDiskEntry != null
                || memoryIterator.hasNext()
                || diskIterator.hasNext();
    }

    @Override
    public BaseEntry<byte[]> next() {
        if (memoryIterator.hasNext() && currentMemoryEntry == null) {
            currentMemoryEntry = memoryIterator.next();
        }
        if (diskIterator.hasNext() && currentDiskEntry == null) {
            currentDiskEntry = diskIterator.next();
        }
        skipNullValues();
        BaseEntry<byte[]> buffer;
        if (currentMemoryEntry == null) {
            buffer = currentDiskEntry;
            currentDiskEntry = null;
        } else if (currentDiskEntry == null) {
            buffer = currentMemoryEntry;
            currentMemoryEntry = null;
        } else {
            int compare = Arrays.compare(currentMemoryEntry.key(), currentDiskEntry.key());
            if (compare > 0) {
                buffer = currentDiskEntry;
                currentDiskEntry = null;
            } else if (compare < 0) {
                buffer = currentMemoryEntry;
                currentMemoryEntry = null;
            } else {
                buffer = currentMemoryEntry;
                currentMemoryEntry = null;
                currentDiskEntry = null;
            }
        }
        return buffer;
    }

    private void skipNullValues() {
        while (currentMemoryEntry != null && currentMemoryEntry.value() == null && memoryIterator.hasNext()) {
            currentMemoryEntry = memoryIterator.next();
        }
        while (currentDiskEntry != null && currentDiskEntry.value() == null && diskIterator.hasNext()) {
            currentDiskEntry = diskIterator.next();
        }
    }
}
