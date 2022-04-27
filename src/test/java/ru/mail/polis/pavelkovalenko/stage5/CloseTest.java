package ru.mail.polis.pavelkovalenko.stage5;

import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CloseTest extends AbstractTest {

    @DaoTest(stage = 5)
    void blockingClose(Dao<String, Entry<String>> dao) throws Exception {
        int count = 2 * N_ENTRIES_FOR_AUTOFLUSH;

        runInParallel(100, count, value -> dao.upsert(entryAt(value))).close();

        long millisElapsed = Timer.elapseMs(dao::close);
        assertTrue(millisElapsed > 150);
    }
}
