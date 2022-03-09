package ru.mail.polis.stepanponomarev.sstable;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Iterator;

final class MappedIterator implements Iterator<Entry<ByteBuffer>> {
    private final MappedByteBuffer mappedTable;

    public MappedIterator(MappedByteBuffer mappedByteBuffer) {
        mappedTable = mappedByteBuffer;
    }

    @Override
    public boolean hasNext() {
        return mappedTable.position() != mappedTable.limit();
    }

    @Override
    public Entry<ByteBuffer> next() {
        final int keySize = mappedTable.getInt();
        final ByteBuffer key = mappedTable.slice(mappedTable.position(), keySize);
        mappedTable.position(mappedTable.position() + keySize);

        final int valueSize = mappedTable.getInt();
        if (valueSize == Utils.TOMBSTONE_TAG) {
            return new BaseEntry<>(key, null);
        }

        final ByteBuffer value = mappedTable.slice(mappedTable.position(), valueSize);
        mappedTable.position(mappedTable.position() + valueSize);

        return new BaseEntry<>(key, value);
    }
}
