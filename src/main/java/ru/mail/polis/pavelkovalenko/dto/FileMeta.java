package ru.mail.polis.pavelkovalenko.dto;

public record FileMeta(byte wasWritten) {

    public static final byte finishedWrite = 1;
    public static final byte unfinishedWrite = 0;
    public static final FileMeta FINISHED_META = new FileMeta(finishedWrite);
    public static final FileMeta UNFINISHED_META = new FileMeta(unfinishedWrite);

    public int size() {
        return Byte.BYTES;
    }
}
