package ru.mail.polis.stepanponomarev.sstable;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;
import ru.mail.polis.stepanponomarev.OSXMemorySegment;

import java.util.Iterator;

final class MappedIterator implements Iterator<Entry<OSXMemorySegment>> {
    private final MemorySegment memorySegment;
    private long position;

    public MappedIterator(MemorySegment segment) {
        memorySegment = segment;
        position = 0;
    }

    @Override
    public boolean hasNext() {
        return memorySegment.byteSize() != position;
    }

    @Override
    public Entry<OSXMemorySegment> next() {
        final long keySize = MemoryAccess.getLongAtOffset(memorySegment, position);
        position += Long.BYTES;

        final MemorySegment key = memorySegment.asSlice(position, keySize);
        position += keySize;

        final long valueSize = MemoryAccess.getLongAtOffset(memorySegment, position);
        position += Long.BYTES;

        if (valueSize == SSTable.TOMBSTONE_TAG) {
            return new BaseEntry<>(new OSXMemorySegment(key), null);
        }

        MemorySegment value = memorySegment.asSlice(position, valueSize);
        position += valueSize;

        return new BaseEntry<>(
                new OSXMemorySegment(key),
                new OSXMemorySegment(value)
        );
    }
}
