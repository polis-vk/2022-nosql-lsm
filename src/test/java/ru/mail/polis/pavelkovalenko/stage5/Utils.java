package ru.mail.polis.pavelkovalenko.stage5;

import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class Utils {

    private static final int FLUSH_TRESHOLD = 1 << 20; // 1 MB

    private static final int INFIMUM_N_BYTES_FOR_ENTRY
            = /*data*/ 2 /*key and value*/ * (Integer.BYTES + 11 /*keysize and valuesize*/)
            + /*index*/ Integer.BYTES;

    private static final int SUPREMUM_N_BYTES_FOR_ENTRY
            = /*data*/ 2 /*key and value*/ * (Long.BYTES + 11 /*keysize and valuesize*/ * Character.BYTES)
            + /*index*/ Long.BYTES;

    private static final int INFIMUM_N_ENTRIES_FOR_AUTOFLUSH
            = (int) Math.ceil((double) FLUSH_TRESHOLD / SUPREMUM_N_BYTES_FOR_ENTRY);

    public static final int N_ENTRIES_FOR_GUARANTEED_AUTOFLUSH
            = (int) Math.ceil((double) FLUSH_TRESHOLD / INFIMUM_N_BYTES_FOR_ENTRY);

    public static final int N_ENTRIES_FOR_ABSENT_AUTOFLUSH = INFIMUM_N_ENTRIES_FOR_AUTOFLUSH - 1;
			
	private Utils() {
	}

    public static void assertNFilesInConfigDir(Dao<String, Entry<String>> dao, int N) {
        AtomicInteger filesCount = new AtomicInteger();
        new ConfigRunnable(dao, filesCount).run();
        assertEquals(N, filesCount.get());
    }

    public static void compactManyTimes(Dao<String, Entry<String>> dao) throws IOException {
        for (int i = 0; i < 5_000; ++i) {
            dao.compact();
        }
    }
}
