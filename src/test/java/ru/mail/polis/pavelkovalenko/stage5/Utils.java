package ru.mail.polis.pavelkovalenko.stage5;

import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class Utils {

    public static final int KEYSIZE_AND_VALUESIZE_TEST_ENTRY = 11;

    private Utils() {
    }

    public static void assertSomeFilesInConfigDir(Dao<String, Entry<String>> dao) {
        AtomicInteger filesCount = new AtomicInteger();
        waitUntilDaoIsFlushing(dao, filesCount);
        assertTrue(filesCount.get() > 0);
    }

    public static void assertNFilesInConfigDir(Dao<String, Entry<String>> dao, int N) {
        AtomicInteger filesCount = new AtomicInteger();
        waitUntilDaoIsFlushing(dao, filesCount);
        assertEquals(N, filesCount.get());
    }

    public static void assertCompactManyTimes(Dao<String, Entry<String>> dao) throws Exception {
        long millisElapsed = Timer.elapseMs(() -> Utils.compactManyTimes(dao));
        assertTrue(millisElapsed < 50);
    }

    private static void waitUntilDaoIsFlushing(Dao<String, Entry<String>> dao, AtomicInteger filesCount) {
        new ConfigRunnable(dao, filesCount).run();
    }

    private static void compactManyTimes(Dao<String, Entry<String>> dao) throws IOException {
        for (int i = 0; i < 5_000; ++i) {
            dao.compact();
        }
    }
}
