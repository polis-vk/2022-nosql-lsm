package ru.mail.polis.stepanponomarev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

public class EntryWithTime implements Entry<OSXMemorySegment> {
    private Entry<OSXMemorySegment> entry;
    private final long timestamp;

    public EntryWithTime(Entry<OSXMemorySegment> entry, long timestamp) {
        this.entry = entry;
        this.timestamp = timestamp;
    }

    public EntryWithTime(OSXMemorySegment key, OSXMemorySegment value, long timestamp) {
        this.entry = new BaseEntry<>(key, value);
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public OSXMemorySegment key() {
        return entry.key();
    }

    @Override
    public OSXMemorySegment value() {
        return entry.value();
    }

    @Override
    public String toString() {
        return "EntryWithTime{" +
                "entry=" + entry +
                ", timestamp=" + timestamp +
                '}';
    }
}
