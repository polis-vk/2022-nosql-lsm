package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
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
        int nThreads = 100;
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
        int nThreads = 100;

        runInParallel(nThreads, count, value -> dao.upsert(entryAt(value))).close();

        long millisElapsed = Timer.elapseMs(() -> {
            for (int i = 0; i < 5_000; ++i) {
                dao.compact();
            }
        });
        assertTrue(millisElapsed < 50);
    }

    @DaoTest(stage = 5)
    void singleCompactWithoutTombstones(Dao<String, Entry<String>> dao) throws Exception {
        Entry<String> entry = entryAt(1);
        dao.upsert(entry);
        dao.flush();

        long millisElapsed = Timer.elapseMs(() -> {
            for (int i = 0; i < 5_000; ++i) {
                dao.compact();
            }
        });
        assertTrue(millisElapsed < 50);

        millisElapsed = Timer.elapseMs(() -> assertSame(dao.get(keyAt(1)), entry));
        assertTrue(millisElapsed < 50);
    }

    @DaoTest(stage = 5)
    void singleCompactWithTombstones(Dao<String, Entry<String>> dao) throws Exception {
        Entry<String> entry = entryAt(1);
        dao.upsert(entry);
        Entry<String> tombstone = new BaseEntry<>(keyAt(2), null);
        dao.upsert(tombstone);
        dao.flush();

        long millisElapsed = Timer.elapseMs(() -> {
            for (int i = 0; i < 5_000; ++i) {
                dao.compact();
            }
        });
        assertTrue(millisElapsed < 50);

        millisElapsed = Timer.elapseMs(() -> {
            assertSame(dao.get(keyAt(1)), entry);
            assertSame(dao.get(keyAt(2)), null);
        });
        assertTrue(millisElapsed < 50);
    }

    @DaoTest(stage = 5)
    void manyCompacts(Dao<String, Entry<String>> dao) throws Exception {
        int count = 10_000;
        int nThreads = 100;
        int nIterations = 3;

        for (int i = 0; i < nIterations; ++i) {
            runInParallel(nThreads, count, value -> dao.upsert(entryAt(value))).close();
            dao.upsert(new BaseEntry<>(keyAt(i), null));
            dao.flush();
        }

        long millisElapsed = Timer.elapseMs(() -> runInParallel(nThreads, count, task -> dao.compact()).close());
        assertTrue(millisElapsed < 50 + nThreads*3);

        millisElapsed = Timer.elapseMs(() -> assertSame(dao.get(keyAt(nIterations - 1)), null));
        assertTrue(millisElapsed < 100);

        millisElapsed = Timer.elapseMs(() -> assertSame(dao.get(keyAt(nIterations)), entryAt(nIterations)));
        assertTrue(millisElapsed < 100);
    }

}
