package ru.mail.polis.pavelkovalenko.dto;

public record FileMeta(byte written, byte tombstoned) {

    public static final byte wasWritten = 1;
    public static final byte wasNotWritten = 0;
    public static final byte hasTombstones = 1;
    public static final byte hasNotTombstones = 0;

    public int size() {
        return Byte.BYTES + Byte.BYTES;
    }
}
