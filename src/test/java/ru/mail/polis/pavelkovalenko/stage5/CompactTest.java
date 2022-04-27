package ru.mail.polis.pavelkovalenko.stage5;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;

import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompactTest extends AbstractTest {

    @DaoTest(stage = 5)
    void backgroundCompact(Dao<String, Entry<String>> dao) throws Exception {
        int count = 2 * N_ENTRIES_FOR_AUTOFLUSH;
        List<Entry<String>> entries = entries("k", "v", count);

        runInParallel(100, count, value -> dao.upsert(entryAt(value))).close();

        Utils.assertSomeFilesInConfigDir(dao);

        long millisElapsed = Timer.elapseMs(dao::compact);
        assertTrue(millisElapsed < 50);

        millisElapsed = Timer.elapseMs(() -> assertSame(dao.all(), entries));
        assertTrue(millisElapsed < 1_500);

        Entry<String> newEntry = entryAt(count + 1);
        millisElapsed = Timer.elapseMs(() -> {
            dao.upsert(newEntry);
            assertSame(dao.get(newEntry.key()), newEntry);
        });
        assertTrue(millisElapsed < 100);
    }

    @DaoTest(stage = 5)
    void emptyCompact(Dao<String, Entry<String>> dao) throws Exception {
        int count = N_ENTRIES_FOR_ABSENT_AUTOFLUSH;

        runInParallel(100, count, value -> dao.upsert(entryAt(value))).close();

        Utils.assertNFilesInConfigDir(dao, 0);
        Utils.assertCompactManyTimes(dao);
        Utils.assertNFilesInConfigDir(dao, 0);
    }

    @DaoTest(stage = 5)
    void singleLightSSTableWithoutTombstones(Dao<String, Entry<String>> dao) throws Exception {
        Entry<String> entry = entryAt(1);
        dao.upsert(entry);
        dao.flush();

        Utils.assertSomeFilesInConfigDir(dao);
        Utils.assertCompactManyTimes(dao);

        long millisElapsed = Timer.elapseMs(() -> assertSame(dao.get(keyAt(1)), entry));
        assertTrue(millisElapsed < 50);
    }

    @DaoTest(stage = 5)
    void singleLightSSTableWithTombstones(Dao<String, Entry<String>> dao) throws Exception {
        Entry<String> entry = entryAt(1);
        dao.upsert(entry);
        Entry<String> tmb = new BaseEntry<>(keyAt(2), null);
        dao.upsert(tmb);
        dao.flush();

        Utils.assertSomeFilesInConfigDir(dao);
        Utils.assertCompactManyTimes(dao);

        long millisElapsed = Timer.elapseMs(() -> {
            assertSame(dao.get(entry.key()), entry);
            assertSame(dao.get(tmb.key()), null);
        });
        assertTrue(millisElapsed < 100);
    }

    @DaoTest(stage = 5)
    void singleHugeSSTableWithoutTombstones(Dao<String, Entry<String>> dao) throws Exception {
        int count = N_ENTRIES_FOR_AUTOFLUSH;
        List<Entry<String>> entries = entries(count);

        runInParallel(100, count, value -> dao.upsert(entryAt(value))).close();

        Utils.assertSomeFilesInConfigDir(dao);
        Utils.assertCompactManyTimes(dao);

        long millisElapsed = Timer.elapseMs(() -> assertSame(dao.all(), entries));
        assertTrue(millisElapsed < 500);
    }

    @DaoTest(stage = 5)
    void singleHugeSSTableWithTombstones(Dao<String, Entry<String>> dao) throws Exception {
        int count = N_ENTRIES_FOR_AUTOFLUSH;
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

        Utils.assertSomeFilesInConfigDir(dao);
        Utils.assertCompactManyTimes(dao);

        long millisElapsed = Timer.elapseMs(() -> assertSame(dao.all(), entries));
        assertTrue(millisElapsed < 750);
    }

    @DaoTest(stage = 5)
    void manySSTables(Dao<String, Entry<String>> dao) throws Exception {
        int count = N_ENTRIES_FOR_ABSENT_AUTOFLUSH;
        int nSSTables = 3;
        String unique = "brilliant";

        List<Entry<String>> uniques = new ArrayList<>(nSSTables);

        for (int i = 0; i < nSSTables; ++i) {
            runInParallel(100, count, value -> dao.upsert(entryAt(value))).close();
            uniques.add(new BaseEntry<>(unique + i, unique));
            dao.upsert(uniques.get(i));
        }

        Utils.assertNFilesInConfigDir(dao, nSSTables * 2);
        Utils.assertCompactManyTimes(dao);

        for (int i = 0; i < nSSTables; ++i) {
            Entry<String> curUnique = uniques.get(i);
            long millisElapsed = Timer.elapseMs(() -> assertSame(dao.get(curUnique.key()), curUnique));
            assertTrue(millisElapsed < 100); // uniques are in the top of all entries
        }
    }
}
