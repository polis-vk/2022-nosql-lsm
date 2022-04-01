package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertNull;

public class CompactTest extends BaseTest {

    private List<Entry<String>> expectedList;

    @DaoTest(stage = 4)
    void compactionFail(Dao<String, Entry<String>> dao) throws IOException, InterruptedException {
        List<Entry<String>> entries = entries(10000);
        expectedList = new ArrayList<>(entries);
        for (int i = 0; i < 10; i++) {
            for (Entry<String> entry : entries) {
                dao.upsert(entry);
            }
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
        }

        int[] overwriteIndexes = new int[]{50, 150, 550, 1050, 1051, 4069, 8888};
        for (int overwriteIndex : overwriteIndexes) {
            Entry<String> overwriteEntry = entry(keyAt(overwriteIndex), "newValue" + overwriteIndex);
            expectedList.set(overwriteIndex, overwriteEntry);
            dao.upsert(overwriteEntry);
        }
        checkCompactFailAfterNanos(dao, 5);
        checkCompactFailAfterNanos(dao, 50);
        checkCompactFailAfterNanos(dao, 250);
        checkCompactFailAfterNanos(dao, 500);
        checkCompactFailAfterNanos(dao, 750);
        checkCompactFailAfterNanos(dao, 999);
    }

    private void checkCompactFailAfterNanos(Dao<String, Entry<String>> dao, int nanos)
            throws IOException, InterruptedException {
        dao.close();
        Dao<String, Entry<String>> finalDao = DaoFactory.Factory.reopen(dao);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean compactionEnded = new AtomicBoolean(false);
        executor.execute(() -> {
            try {
                finalDao.compact();
                compactionEnded.set(true);
                finalDao.close();
            } catch (IOException e) {
                assertNull(e);
            }
        });
        Thread.sleep(0, nanos);
        executor.shutdownNow();
        if (!compactionEnded.get()) {
            assertSame(finalDao.all(), expectedList);
        }
    }
}
