package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.util.List;

public class Stage4IdempotentTest extends BaseTest {

    @DaoTest(stage = 4)
    void closeEmpty(Dao<String, Entry<String>> dao) throws IOException {
        for (int i = 0; i < 5000; i++) {
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
        }
    }

    @DaoTest(stage = 4)
    void closeSimple(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        dao.upsert(entryAt(10));
        for (int i = 0; i < 5000; i++) {
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
        }
        assertSame(dao.all(), entryAt(1), entryAt(10));
    }

    @DaoTest(stage = 4)
    void close(Dao<String, Entry<String>> dao) throws IOException {
        List<Entry<String>> entries = entries(103);
        int splitIndex = 60;
        for (Entry<String> entry : entries.subList(0, splitIndex)) {
            dao.upsert(entry);
        }
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        for (Entry<String> entry : entries.subList(splitIndex, entries.size())) {
            dao.upsert(entry);
        }
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        Entry<String> firstNewEntry = entry(keyAt(1), valueAt(1001));
        Entry<String> lastNewEntry = entry(keyAt(103), valueAt(1003));
        dao.upsert(firstNewEntry);
        dao.upsert(lastNewEntry);

        assertSame(dao.get(firstNewEntry.key()), firstNewEntry);
        assertSame(dao.get(keyAt(2), keyAt(103)), entries.subList(2, 103));
        assertSame(dao.get(lastNewEntry.key()), lastNewEntry);

        for (int i = 0; i < 5000; i++) {
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
        }
        assertSame(dao.get(firstNewEntry.key()), firstNewEntry);
        assertSame(dao.get(keyAt(2), keyAt(103)), entries.subList(2, 103));
        assertSame(dao.get(lastNewEntry.key()), lastNewEntry);
    }

    @DaoTest(stage = 4)
    void compactEmpty(Dao<String, Entry<String>> dao) throws IOException {
        for (int i = 0; i < 5000; i++) {
            dao.compact();
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
        }
    }

    @DaoTest(stage = 4)
    void compactSimple(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        dao.upsert(entryAt(10));
        for (int i = 0; i < 5000; i++) {
            dao.compact();
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
        }
        assertSame(dao.all(), entryAt(1), entryAt(10));
    }

    @DaoTest(stage = 4)
    void compactManyTimes(Dao<String, Entry<String>> dao) throws IOException {
        for (int i = 0; i < 100; i++) {
            dao.upsert(entryAt(1));
            dao.upsert(entryAt(10));
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
            dao.compact();
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
        }
        assertSame(dao.all(), entryAt(1), entryAt(10));
    }

    @DaoTest(stage = 4)
    void compact(Dao<String, Entry<String>> dao) throws IOException {
        List<Entry<String>> entries = entries(103);
        int splitIndex = 60;
        for (Entry<String> entry : entries.subList(0, splitIndex)) {
            dao.upsert(entry);
        }
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        for (Entry<String> entry : entries.subList(splitIndex, entries.size())) {
            dao.upsert(entry);
        }
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        Entry<String> firstNewEntry = entry(keyAt(1), valueAt(1001));
        Entry<String> lastNewEntry = entry(keyAt(103), valueAt(1003));
        dao.upsert(firstNewEntry);
        dao.upsert(lastNewEntry);

        assertSame(dao.get(firstNewEntry.key()), firstNewEntry);
        assertSame(dao.get(keyAt(2), keyAt(103)), entries.subList(2, 103));
        assertSame(dao.get(lastNewEntry.key()), lastNewEntry);

        for (int i = 0; i < 5000; i++) {
            dao.compact();
            dao.close();
            dao = DaoFactory.Factory.reopen(dao);
        }
        assertSame(dao.get(firstNewEntry.key()), firstNewEntry);
        assertSame(dao.get(keyAt(2), keyAt(103)), entries.subList(2, 103));
        assertSame(dao.get(lastNewEntry.key()), lastNewEntry);
    }
}
