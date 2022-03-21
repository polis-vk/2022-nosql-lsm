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
        if (nextMemoryEntry != null && nextMemoryEntry.value() == null && nextDiskEntry == null) {
            while (memoryIterator.hasNext() && nextMemoryEntry != null && nextMemoryEntry.value() == null) {
                nextMemoryEntry = memoryIterator.next();
            }
            if (nextMemoryEntry != null && nextMemoryEntry.value() == null && nextDiskEntry != null
                    && Arrays.compare(nextMemoryEntry.key(), nextDiskEntry.key()) == 0) {
                return false;
            }
            if (nextMemoryEntry != null && nextMemoryEntry.value() == null) {
                nextMemoryEntry = null;
            }
        }
        if (nextDiskEntry != null && nextDiskEntry.value() == null && nextMemoryEntry == null) {
            while (diskIterator.hasNext() && nextDiskEntry != null && nextDiskEntry.value() == null) {
                nextDiskEntry = diskIterator.next();
            }
            if (nextDiskEntry != null && nextDiskEntry.value() == null && nextMemoryEntry != null
                    && Arrays.compare(nextMemoryEntry.key(), nextDiskEntry.key()) == 0) {
                return false;
            }
            if (nextDiskEntry != null && nextDiskEntry.value() == null) {
                nextDiskEntry = null;
            }
        }
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
        if (nextDiskEntry != null && nextMemoryEntry != null) {
            int compare = Arrays.compare(nextMemoryEntry.key(), nextDiskEntry.key());
            if (compare > 0) {
                if (nextDiskEntry.value() == null) {
                    while (diskIterator.hasNext() && nextDiskEntry != null && nextDiskEntry.value() == null
                            && Arrays.compare(nextMemoryEntry.key(), nextDiskEntry.key()) > 0) {
                        nextDiskEntry = diskIterator.next();
                    }
                    return next();
                } else {
                    buffer = nextDiskEntry;
                    if (diskIterator.hasNext()) {
                        nextDiskEntry = diskIterator.next();
                    } else {
                        nextDiskEntry = null;
                    }
                    return buffer;
                }
            } else {
                if (nextMemoryEntry.value() == null) {
                    while (memoryIterator.hasNext() && nextMemoryEntry != null && nextMemoryEntry.value() == null
                            && Arrays.compare(nextMemoryEntry.key(), nextDiskEntry.key()) < 0) {
                        nextMemoryEntry = memoryIterator.next();
                    }
                    if (nextMemoryEntry != null && nextMemoryEntry.value() == null) {
                        nextMemoryEntry = null;
                    }
                    if (nextDiskEntry != null && nextDiskEntry.value() != null) {
                        buffer = nextDiskEntry;
                        if (diskIterator.hasNext()) {
                            nextDiskEntry = diskIterator.next();
                        } else {
                            nextDiskEntry = null;
                        }
                        return buffer;
                    }
                    return next();
                } else {
                    buffer = nextMemoryEntry;
                    if (memoryIterator.hasNext()) {
                        nextMemoryEntry = memoryIterator.next();
                    } else {
                        nextMemoryEntry = null;
                    }
                    if (compare == 0) {
                        if (diskIterator.hasNext()) {
                            nextDiskEntry = diskIterator.next();
                        } else {
                            nextDiskEntry = null;
                        }
                    }
                    return buffer;
                }
            }
        } else if (nextMemoryEntry == null) {
            if (nextDiskEntry != null && nextDiskEntry.value() == null) {
                while (diskIterator.hasNext() && nextDiskEntry != null && nextDiskEntry.value() == null) {
                    nextDiskEntry = diskIterator.next();
                }
                if (nextDiskEntry != null && nextDiskEntry.value() == null) {
                    nextDiskEntry = null;
                }
                return null;
            } else {
                buffer = nextDiskEntry;
                if (diskIterator.hasNext()) {
                    nextDiskEntry = diskIterator.next();
                } else {
                    nextDiskEntry = null;
                }
                return buffer;
            }
        } else {
            if (nextMemoryEntry.value() == null) {
                while (memoryIterator.hasNext() && nextMemoryEntry != null && nextMemoryEntry.value() == null) {
                    nextMemoryEntry = memoryIterator.next();
                }
                if (nextMemoryEntry != null && nextMemoryEntry.value() == null) {
                    nextMemoryEntry = null;
                }
                return null;
            } else {
                buffer = nextMemoryEntry;
                if (memoryIterator.hasNext()) {
                    nextMemoryEntry = memoryIterator.next();
                } else {
                    nextMemoryEntry = null;
                }
                return buffer;
            }
        }
    }
}
