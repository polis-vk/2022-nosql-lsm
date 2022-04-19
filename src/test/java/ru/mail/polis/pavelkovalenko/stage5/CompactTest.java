package ru.mail.polis.pavelkovalenko.stage5;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompactTest extends BaseTest {

    @DaoTest(stage = 5)
    void backgroundCompact(Dao<String, Entry<String>> dao) throws Exception {
        int count = 3 * Utils.N_ENTRIES_FOR_FLUSH;
        List<Entry<String>> entries = entries("k", "v", count);

        runInParallel(100, count, value -> dao.upsert(entryAt(value))).close();

        Thread.sleep(1_000); // wait until dao is flushing

        long millisElapsed = Timer.elapseMs(dao::compact);
        assertTrue(millisElapsed < 50);

        millisElapsed = Timer.elapseMs(() -> assertSame(dao.all(), entries));
        assertTrue(millisElapsed < 1_500);

        Entry<String> newEntry = entryAt(count + 1);
        millisElapsed = Timer.elapseMs(() -> {
            dao.upsert(newEntry);
            assertSame(dao.get(newEntry.key()), newEntry);
        });
        assertTrue(millisElapsed < 50);
    }

    @DaoTest(stage = 5)
    void emptyCompactFromDisk(Dao<String, Entry<String>> dao) throws Exception {
        int count = Utils.N_ENTRIES_FOR_FLUSH - 1;
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
    void singleSSTableWithoutTombstones(Dao<String, Entry<String>> dao) throws Exception {
        Entry<String> entry = entryAt(1);
        dao.upsert(entry);
        dao.flush();

        Thread.sleep(100); // wait until dao is flushing

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
    void singleSSTableWithTombstones(Dao<String, Entry<String>> dao) throws Exception {
        Entry<String> entry = entryAt(1);
        dao.upsert(entry);
        Entry<String> tmb = new BaseEntry<>(keyAt(2), null);
        dao.upsert(tmb);
        dao.flush();

        Thread.sleep(100); // wait until dao is flushing

        long millisElapsed = Timer.elapseMs(() -> {
            for (int i = 0; i < 5_000; ++i) {
                dao.compact();
            }
        });
        assertTrue(millisElapsed < 50);

        millisElapsed = Timer.elapseMs(() -> {
            assertSame(dao.get(entry.key()), entry);
            assertSame(dao.get(tmb.key()), null);
        });
        assertTrue(millisElapsed < 50);
    }

    @DaoTest(stage = 5)
    void singleHugeSSTableWithoutTombstones(Dao<String, Entry<String>> dao) throws Exception {
        int count = Utils.N_ENTRIES_FOR_FLUSH;
        List<Entry<String>> entries = entries(count);

        runInParallel(100, count, value -> dao.upsert(entryAt(value))).close();
        Thread.sleep(1_000); // wait until dao is flushing

        long millisElapsed = Timer.elapseMs(() -> {
            for (int i = 0; i < 5_000; ++i) {
                dao.compact();
            }
        });
        assertTrue(millisElapsed < 50);

        millisElapsed = Timer.elapseMs(() -> assertSame(dao.all(), entries));
        assertTrue(millisElapsed < 500);
    }

    @DaoTest(stage = 5)
    void singleHugeSSTableWithTombstones(Dao<String, Entry<String>> dao) throws Exception {
        int count = Utils.N_ENTRIES_FOR_FLUSH;
        List<Entry<String>> entries = new ArrayList<>();
        for (int i = 0; i < count; ++i) {
            Entry<String> curEntry;
            if (i % 2 == 0) {
                curEntry = entryAt(i);
                entries.add(curEntry);
            } else {
                curEntry = new BaseEntry<>(keyAt(i), null);
            }
            dao.upsert(curEntry);
        }

        Thread.sleep(1_000); // wait until dao is flushing

        long millisElapsed = Timer.elapseMs(() -> {
            for (int i = 0; i < 5_000; ++i) {
                dao.compact();
            }
        });
        assertTrue(millisElapsed < 50);

        millisElapsed = Timer.elapseMs(() -> assertSame(dao.all(), entries));
        assertTrue(millisElapsed < 750);
    }

    @DaoTest(stage = 5)
    void manySSTables(Dao<String, Entry<String>> dao) throws Exception {
        int count = Utils.N_ENTRIES_FOR_FLUSH - 1;
        int nIterations = 3;

        for (int i = 0; i < nIterations; ++i) {
            runInParallel(100, count, value -> dao.upsert(entryAt(value))).close();
            dao.upsert(new BaseEntry<>(keyAt(i), null));
            dao.flush();
        }

        Thread.sleep(1_500); // wait until dao is flushing

        long millisElapsed = Timer.elapseMs(() -> {
            for (int i = 0; i < 5_000; ++i) {
                dao.compact();
            }
        });
        assertTrue(millisElapsed < 50);

        for (int i = 0; i < nIterations; ++i) {
            String tmb = keyAt(i);
            millisElapsed = Timer.elapseMs(() -> assertSame(dao.get(tmb), null));
            assertTrue(millisElapsed < 100);
        }

        millisElapsed = Timer.elapseMs(() -> assertSame(dao.get(keyAt(nIterations)), entryAt(nIterations)));
        assertTrue(millisElapsed < 100);
    }

}
