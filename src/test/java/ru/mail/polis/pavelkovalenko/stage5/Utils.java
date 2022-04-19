package ru.mail.polis.pavelkovalenko.stage5;

public class Utils {

    private static final int FLUSH_TRESHOLD = 1 << 20; // 1 MB
    private static final int N_BYTES_FOR_ENTRY
            = 11 /*keysize and valuesize*/ * Character.BYTES * 2 /*key and value*/;
    public static final int N_ENTRIES_FOR_FLUSH = (int) Math.ceil((double) FLUSH_TRESHOLD / N_BYTES_FOR_ENTRY);
}
