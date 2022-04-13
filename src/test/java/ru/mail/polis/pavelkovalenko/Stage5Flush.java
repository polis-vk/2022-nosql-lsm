package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;

import java.util.List;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class Stage5Flush extends BaseTest {

    private final Timer timer = Timer.INSTANSE;

    @DaoTest(stage = 5)
    void backgroundFlush(Dao<String, Entry<String>> dao) throws Exception {
        int count = 20_000; // 1 entry = 4 + 10*2 + 4 + 10*2 = 48 Byte, 20_000 entry ~~ 940 KB < 1 MB
        List<Entry<String>> entries = entries("k", "v", count);

        runInParallel(100, count, value -> dao.upsert(entries.get(value))).close();

        timer.set();
        dao.flush();
        long millisElapsed = timer.elapse();
        assert(millisElapsed < 50);

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
    void flushOverfill(Dao<String, Entry<String>> dao) {
        int count = 50_000; // > 2 MB
        List<Entry<String>> entries = entries("k", "v", count);

        assertThrows(Exception.class,
                () -> runInParallel(100, count, value -> dao.upsert(entries.get(value))).close());
    }

    @DaoTest(stage = 5)
    void manyFlushes(Dao<String, Entry<String>> dao) throws Exception {
        int count = 10_000;
        final int nThreads = 100;
        List<Entry<String>> entries = entries("k", "v", count);

        runInParallel(nThreads, count, value -> dao.upsert(entries.get(value))).close();

        timer.set();
        runInParallel(nThreads, task -> dao.flush()).close();
        long millisElapsed = timer.elapse();
        assert(millisElapsed < 50 + nThreads*3);
    }

}
