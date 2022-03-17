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
    }

    @Override
    public boolean hasNext() {
        return currentMemoryEntry != null
                || currentDiskEntry != null
                || memoryIterator.hasNext()
                || diskIterator.hasNext();
    }

    @Override
    public BaseEntry<byte[]> next() {
        if (memoryIterator.hasNext()) {
            if (currentMemoryEntry == null) {
                currentMemoryEntry = memoryIterator.next();
            }
        }
        if (diskIterator.hasNext()) {
            if (currentDiskEntry == null) {
                currentDiskEntry = diskIterator.next();
            }
        }
        BaseEntry<byte[]> buffer;
        if (currentMemoryEntry != null && currentDiskEntry != null) {
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
        } else if (currentMemoryEntry != null) {
            buffer = currentMemoryEntry;
            currentMemoryEntry = null;
        } else {
            buffer = currentDiskEntry;
            currentDiskEntry = null;
        }
        return buffer;
    }
}
