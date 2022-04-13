package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;

import java.util.Collections;
import java.util.List;

public class Stage5Compact extends BaseTest {

    private final Timer timer = Timer.INSTANSE;

    @DaoTest(stage = 5)
    void backgroundCompact(Dao<String, Entry<String>> dao) throws Exception {
        int count = 10_000;
        final int nThreads = 100;
        List<Entry<String>> entries = entries("k", "v", count);

        for (int i = 0; i < 3; ++i) {
            runInParallel(nThreads, count, value -> dao.upsert(entries.get(value))).close();
            dao.flush();
        }

        timer.set();
        dao.compact();
        long millisElapsed = timer.elapse();
        assert (millisElapsed < 50);

        timer.set();
        assertSame(dao.all(), entries);
        millisElapsed = timer.elapse();
        assert(millisElapsed < 1_000);

        Entry<String> newEntry = entryAt(count + 1);
        timer.set();
        dao.upsert(newEntry);
        assertSame(dao.get(newEntry.key()), newEntry);
        millisElapsed = timer.elapse();
        assert (millisElapsed < 50);
    }

    @DaoTest(stage = 5)
    void emptyCompactFromDisk(Dao<String, Entry<String>> dao) throws Exception {
        int count = 20_000;
        final int nThreads = 100;
        List<Entry<String>> entries = entries("k", "v", count);

        runInParallel(nThreads, count, value -> dao.upsert(entries.get(value))).close();

        timer.set();
        for (int i = 0; i < 5_000; ++i) {
            dao.compact();
        }
        long millisElapsed = timer.elapse();
        assert(millisElapsed < 50);

        Thread.sleep(500);
        runInParallel(nThreads, count, value -> dao.upsert(new BaseEntry<>(entries.get(value).key(), null))).close();
        assertSame(dao.all(), Collections.emptyList());
    }

    @DaoTest(stage = 5)
    void manyCompacts(Dao<String, Entry<String>> dao) throws Exception {
        int count = 10_000;
        final int nThreads = 100;
        List<Entry<String>> entries = entries("k", "v", count);

        runInParallel(nThreads, count, value -> dao.upsert(entries.get(value))).close();
        dao.flush();

        timer.set();
        runInParallel(nThreads, task -> dao.compact());
        long millisElapsed = timer.elapse();
        assert(millisElapsed < 50 + nThreads*3);
    }

}
