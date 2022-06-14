package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Entry;
import ru.mail.polis.vladislavfetisov.lsm.SSTable;

import java.util.Iterator;

public final class Entries {
    public static final long MIN_LENGTH = 2L * Long.BYTES;

    private Entries() {

    }

    public static long sizeOfEntry(Entry<MemorySegment> entry) {
        long valueSize = getValueSize(entry);
        return 2L * Long.BYTES + entry.key().byteSize() + valueSize;
    }

    private static long getValueSize(Entry<MemorySegment> entry) {
        return entry.isTombstone() ? 0 : entry.value().byteSize();
    }

    public static SSTable.Sizes getSizes(Iterator<Entry<MemorySegment>> values) {
        long tableSize = 0;
        long count = 0;
        while (values.hasNext()) {
            tableSize += sizeOfEntry(values.next());
            count++;
        }
        return new SSTable.Sizes(tableSize, count * Long.BYTES);
    }

}
