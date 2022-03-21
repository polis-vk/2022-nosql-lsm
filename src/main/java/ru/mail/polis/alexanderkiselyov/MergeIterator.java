package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;

import java.util.Arrays;
import java.util.Iterator;

public class MergeIterator implements Iterator<BaseEntry<byte[]>> {
    private final Iterator<BaseEntry<byte[]>> memoryIterator;
    private final Iterator<BaseEntry<byte[]>> diskIterator;
    private BaseEntry<byte[]> nextMemoryEntry;
    private BaseEntry<byte[]> nextDiskEntry;

    public MergeIterator(Iterator<BaseEntry<byte[]>> memoryIterator, Iterator<BaseEntry<byte[]>> diskIterator) {
        this.memoryIterator = memoryIterator;
        this.diskIterator = diskIterator;
        nextMemoryEntry = memoryIterator.hasNext() ? memoryIterator.next() : null;
        nextDiskEntry = diskIterator.hasNext() ? diskIterator.next() : null;
    }

    @Override
    public boolean hasNext() {
        missMemoryNullValues();
        missDiskNullValues();
        if (nextMemoryEntry != null && nextMemoryEntry.value() == null && nextDiskEntry != null
                && Arrays.compare(nextMemoryEntry.key(), nextDiskEntry.key()) == 0) {
            if (!memoryIterator.hasNext() && !diskIterator.hasNext()) {
                return false;
            }
            if (memoryIterator.hasNext()) {
                nextMemoryEntry = memoryIterator.next();
            }
            if (diskIterator.hasNext()) {
                nextDiskEntry = diskIterator.next();
            }
            return hasNext();
        }
        return nextMemoryEntry != null || nextDiskEntry != null;
    }

    @Override
    public BaseEntry<byte[]> next() {
        BaseEntry<byte[]> buffer;
        if (nextDiskEntry == null && nextMemoryEntry != null) {
            if (nextMemoryEntry.value() == null) {
                missMemoryNullValues();
                return null;
            } else {
                buffer = nextMemoryEntry;
                getNextMemoryEntry();
                return buffer;
            }
        } else if (nextMemoryEntry == null && nextDiskEntry != null) {
            if (nextDiskEntry.value() == null) {
                missDiskNullValues();
                return null;
            } else {
                buffer = nextDiskEntry;
                getNextDiskEntry();
                return buffer;
            }
        } else {
            int compare = 0;
            if (nextMemoryEntry != null) {
                compare = Arrays.compare(nextMemoryEntry.key(), nextDiskEntry.key());
            }
            if (compare > 0) {
                if (nextDiskEntry.value() == null) {
                    skipSmallValues(true);
                    return next();
                } else {
                    buffer = nextDiskEntry;
                    getNextDiskEntry();
                    return buffer;
                }
            } else {
                if (nextMemoryEntry != null) {
                    if (nextMemoryEntry.value() == null) {
                        skipSmallValues(false);
                        return next();
                    } else {
                        buffer = nextMemoryEntry;
                        getNextMemoryEntry();
                        if (compare == 0) {
                            getNextDiskEntry();
                        }
                        return buffer;
                    }
                }
                return null;
            }
        }
    }

    private void missMemoryNullValues() {
        if (nextMemoryEntry != null && nextMemoryEntry.value() == null && nextDiskEntry == null) {
            while (memoryIterator.hasNext() && nextMemoryEntry != null && nextMemoryEntry.value() == null) {
                nextMemoryEntry = memoryIterator.next();
            }
            if (nextMemoryEntry != null && nextMemoryEntry.value() == null) {
                nextMemoryEntry = null;
            }
        }
    }

    private void missDiskNullValues() {
        if (nextDiskEntry != null && nextDiskEntry.value() == null && nextMemoryEntry == null) {
            while (diskIterator.hasNext() && nextDiskEntry != null && nextDiskEntry.value() == null) {
                nextDiskEntry = diskIterator.next();
            }
            if (nextDiskEntry != null && nextDiskEntry.value() == null) {
                nextDiskEntry = null;
            }
        }
    }

    private void getNextDiskEntry() {
        if (diskIterator.hasNext()) {
            nextDiskEntry = diskIterator.next();
        } else {
            nextDiskEntry = null;
        }
    }

    private void getNextMemoryEntry() {
        if (memoryIterator.hasNext()) {
            nextMemoryEntry = memoryIterator.next();
        } else {
            nextMemoryEntry = null;
        }
    }

    private void skipSmallValues(boolean isLarger) {
        int compare = Arrays.compare(nextMemoryEntry.key(), nextDiskEntry.key());
        if (isLarger) {
            while (diskIterator.hasNext() && nextDiskEntry != null && nextDiskEntry.value() == null
                    && compare > 0) {
                nextDiskEntry = diskIterator.next();
            }
        } else {
            while (memoryIterator.hasNext() && nextMemoryEntry != null && nextMemoryEntry.value() == null
                    && compare < 0) {
                nextMemoryEntry = memoryIterator.next();
            }
        }
        if (nextMemoryEntry != null && nextMemoryEntry.value() == null) {
            nextMemoryEntry = null;
        }
    }
}
