package ru.mail.polis.pavelkovalenko.stage5;

import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;

import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UpsertTest extends AbstractTest {

    @DaoTest(stage = 5)
    void noAutoflush(Dao<String, Entry<String>> dao) throws Exception {
        int count = N_ENTRIES_FOR_ABSENT_AUTOFLUSH;

        runInParallel(100, count, value -> dao.upsert(entryAt(value))).close();

        Utils.assertNFilesInConfigDir(dao, 0);

        dao.upsert(entryAt(1));

        Utils.assertNFilesInConfigDir(dao, 0);
    }

    @DaoTest(stage = 5)
    void autoflush(Dao<String, Entry<String>> dao) throws Exception {
        int count = N_ENTRIES_FOR_AUTOFLUSH;

        runInParallel(100, count, value -> dao.upsert(entryAt(value))).close();

        Utils.assertNFilesInConfigDir(dao, 2);
    }

    @DaoTest(stage = 5)
    void nonBlockingUpsert(Dao<String, Entry<String>> dao) throws Exception {
        int count = 2 * N_ENTRIES_FOR_ABSENT_AUTOFLUSH;
        List<Entry<String>> oneHalf = new ArrayList<>(count / 2);
        for (int i = 0; i < count / 2; ++i) {
            oneHalf.add(entryAt(i));
        }
        List<Entry<String>> twoHalf = new ArrayList<>(count / 2);
        for (int i = count / 2; i < count; ++i) {
            twoHalf.add(entryAt(i));
        }

        long millisElapsed1 = Timer
                .elapseMs(() -> runInParallel(100, count, value -> dao.upsert(oneHalf.get(value))).close());

        long millisElapsed2 = Timer
                .elapseMs(() -> runInParallel(100, count, value -> dao.upsert(twoHalf.get(value))).close());

        long delta = Math.abs(millisElapsed1 - millisElapsed2);
        assertTrue(delta < 50);
    }
}
