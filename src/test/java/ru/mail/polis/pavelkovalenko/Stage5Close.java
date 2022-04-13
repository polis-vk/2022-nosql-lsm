package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;

import java.util.List;

public class Stage5Close extends BaseTest {

    private final Timer timer = Timer.INSTANSE;

    @DaoTest(stage = 5)
    void blockingClose(Dao<String, Entry<String>> dao) throws Exception {
        int count = 40_000;
        final int nThreads = 100;
        List<Entry<String>> entries = entries("k", "v", count);

        runInParallel(nThreads, count, value -> dao.upsert(entries.get(value))).close();
        dao.flush();

        timer.set();
        dao.close();
        long millisElapsed = timer.elapse();
        assert(millisElapsed > 500);
    }

}
