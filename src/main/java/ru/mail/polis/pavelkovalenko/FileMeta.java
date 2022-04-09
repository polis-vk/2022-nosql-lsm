package ru.mail.polis.pavelkovalenko;

public record FileMeta(byte wasWritten) {

    public static final byte unfinishedWrite = 0;
    public static final byte finishedWrite = 1;

    public int size() {
        return Byte.BYTES + Character.BYTES;
    }
}
