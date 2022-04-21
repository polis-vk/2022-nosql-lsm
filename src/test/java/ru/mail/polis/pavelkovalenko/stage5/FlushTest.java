package ru.mail.polis.pavelkovalenko.stage5;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;

import java.util.List;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlushTest extends BaseTest {

    @DaoTest(stage = 5)
    void backgroundFlush(Dao<String, Entry<String>> dao) throws Exception {
        int count = Utils.N_ENTRIES_FOR_FLUSH;
        List<Entry<String>> entries = entries("k", "v", count);

        runInParallel(100, count, value -> dao.upsert(entryAt(value))).close();

        long millisElapsed = Timer.elapseMs(dao::flush);
        assertTrue(millisElapsed < 50);

        millisElapsed = Timer.elapseMs(() -> assertSame(dao.all(), entries));
        assertTrue(millisElapsed < 1_000);

        Entry<String> newEntry = entryAt(count + 1);
        millisElapsed = Timer.elapseMs(() -> {
            dao.upsert(newEntry);
            assertSame(dao.get(newEntry.key()), newEntry);
        });
        assertTrue(millisElapsed < 50);
    }

    @DaoTest(stage = 5)
    void flushOverfill(Dao<String, Entry<String>> dao) {
        int count = 100 * Utils.N_ENTRIES_FOR_FLUSH;

        assertThrows(Exception.class,
                () -> runInParallel(100, count, value -> dao.upsert(entryAt(value))).close());
    }

    @DaoTest(stage = 5)
    void manyFlushes(Dao<String, Entry<String>> dao) throws Exception {
        int count = Utils.N_ENTRIES_FOR_FLUSH - 1;
        int nThreads = 100;

        runInParallel(nThreads, count, value -> dao.upsert(entryAt(value))).close();

        long millisElapsed = Timer.elapseMs(() -> runInParallel(nThreads, task -> dao.flush()).close());
        assertTrue(millisElapsed < 100);
    }

}
