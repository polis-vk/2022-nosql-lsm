package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;
import ru.mail.polis.test.DaoFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Stage5Test extends BaseTest {

    @DaoTest(stage = 5)
    void asyncFlushTest(Dao<String, Entry<String>> dao) throws Exception {
        int count = 40000;
        List<Entry<String>> entries1 = entries("k", "v", count);
        List<Entry<String>> entries2 = entries("q", "w", count);
        entries1.forEach(dao::upsert);
        dao.flush();
        for (Entry<String> entry : entries2) {
            dao.upsert(entry);
        }
        assertSame(dao.allTo("q0000000000"), entries1);
        assertSame(dao.allFrom("q0000000000"), entries2);
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.allTo("q0000000000"), entries1);
        assertSame(dao.allFrom("q0000000000"), entries2);
    }

    @DaoTest(stage = 5)
    void concurrentUpsertOnFlushTest(Dao<String, Entry<String>> dao) throws Exception {
        int threadsNumber = 30;
        int count = threadsNumber * 1000;
        List<Entry<String>> entries1 = entries("k", "v", count);
        List<Entry<String>> entries2 = entries("q", "w", count);
        CountDownLatch countDownLatch = new CountDownLatch(threadsNumber);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadsNumber; i++) {
            int finalI = i;
            threads.add(new Thread(() -> {
                List<Entry<String>> entries = entries2.subList(finalI * 1000, (finalI + 1) * 1000);
                entries.forEach(dao::upsert);
                countDownLatch.countDown();
            }));
        }

        entries1.forEach(dao::upsert);
        dao.flush();
        threads.forEach(Thread::start);
        countDownLatch.await();
        assertSame(dao.allTo("q0000000000"), entries1);
        assertSame(dao.allFrom("q0000000000"), entries2);
        dao.close();
        Dao<String, Entry<String>> reopenDao = DaoFactory.Factory.reopen(dao);
        assertSame(reopenDao.allTo("q0000000000"), entries1);
        assertSame(reopenDao.allFrom("q0000000000"), entries2);
    }

    @DaoTest(stage = 5)
    void autoFlushTest(Dao<String, Entry<String>> dao) throws Exception {
        int count = 100_000;
        List<Entry<String>> entries = entries(count);
        entries.forEach(dao::upsert);
        assertSame(dao.all(), entries);
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.all(), entries);
    }
}
