package ru.mail.polis.pavelkovalenko.stage5;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class Utils {

    public static final int KEYSIZE_AND_VALUESIZE_TEST_ENTRY = new BaseTest().keyAt(0).length();

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
