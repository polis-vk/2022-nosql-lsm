package ru.mail.polis.pavelkovalenko.stage5;

public class Utils {

    private static final int FLUSH_TRESHOLD = 1 << 20; // 1 MB

    private static final int SUPREMUM_N_BYTES_FOR_ENTRY
            = /*data*/ 2 /*key and value*/ * (Long.BYTES + 11 /*keysize and valuesize*/ * Character.BYTES)
            + /*index*/ Long.BYTES;

    private static final int INFIMUM_N_BYTES_FOR_ENTRY
            = /*data*/ 2 /*key and value*/ * (Integer.BYTES + 11 /*keysize and valuesize*/)
            + /*index*/ Integer.BYTES;

    public static final int SUPREMUM_N_ENTRIES_FOR_FLUSH
            = (int) Math.ceil((double) FLUSH_TRESHOLD / INFIMUM_N_BYTES_FOR_ENTRY);

    public static final int INFIMUM_N_ENTRIES_FOR_FLUSH
            = (int) Math.ceil((double) FLUSH_TRESHOLD / SUPREMUM_N_BYTES_FOR_ENTRY);
}
