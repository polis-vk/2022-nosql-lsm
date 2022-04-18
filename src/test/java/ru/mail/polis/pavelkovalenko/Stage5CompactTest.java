package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class Stage5CompactTest extends BaseTest {

    @DaoTest(stage = 5)
    void backgroundCompact(Dao<String, Entry<String>> dao) throws Exception {
        int count = 10_000;
        final int nThreads = 100;
        List<Entry<String>> entries = entries("k", "v", count);

        for (int i = 0; i < 3; ++i) {
            runInParallel(nThreads, count, value -> dao.upsert(entryAt(value))).close();
            dao.flush();
        }

        long millisElapsed = Timer.elapseMs(dao::compact);
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
    void emptyCompactFromDisk(Dao<String, Entry<String>> dao) throws Exception {
        int count = 20_000;
        final int nThreads = 100;

        runInParallel(nThreads, count, value -> dao.upsert(entryAt(value))).close();

        long millisElapsed = Timer.elapseMs(() -> {
            for (int i = 0; i < 5_000; ++i) {
                dao.compact();
            }
        });
        assertTrue(millisElapsed < 50);
    }

    @DaoTest(stage = 5)
    void manyCompacts(Dao<String, Entry<String>> dao) throws Exception {
        int count = 10_000;
        final int nThreads = 100;

        runInParallel(nThreads, count, value -> dao.upsert(entryAt(value))).close();
        dao.flush();

        long millisElapsed = Timer.elapseMs(() -> runInParallel(nThreads, task -> dao.compact()));
        assertTrue(millisElapsed < 50 + nThreads*3);
    }

}
