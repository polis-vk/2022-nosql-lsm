package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class Stage5CloseTest extends BaseTest {

    @DaoTest(stage = 5)
    void blockingClose(Dao<String, Entry<String>> dao) throws Exception {
        int count = 40_000;
        final int nThreads = 100;
        List<Entry<String>> entries = entries("k", "v", count);

        runInParallel(nThreads, count, value -> dao.upsert(entries.get(value))).close();
        dao.flush();

        long millisElapsed = Timer.elapseMs(dao::close);
        assertTrue(millisElapsed > 500);
    }

}
