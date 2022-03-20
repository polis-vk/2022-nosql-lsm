package ru.mail.polis.daniilbakin;

import java.io.IOException;
import java.util.Iterator;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;
import ru.mail.polis.test.DaoFactory;

public class UpsertRemoveTest extends BaseTest {

    @DaoTest(stage = 3)
    void persistentRemoveTest(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(1), null));
        assertSame(dao.get(keyAt(1)), null);
    }

    @DaoTest(stage = 3)
    void persistentRemoveTestRange(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        dao.upsert(entryAt(2));
        dao.upsert(new BaseEntry<>(keyAt(2), null));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);

        assertSame(dao.all(), entryAt(1));
    }

    @DaoTest(stage = 3)
    void persistentRemoveTestRangeInMemory(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        dao.upsert(entryAt(2));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(2), null));

        assertSame(dao.all(), entryAt(1));
    }

    @DaoTest(stage = 3)
    void persistentGetAfterRemoveTestRange(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        dao.upsert(entryAt(2));
        dao.upsert(new BaseEntry<>(keyAt(2), null));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entryAt(2));

        assertSame(dao.all(), entryAt(1), entryAt(2));
    }

    @DaoTest(stage = 3)
    void persistentGetAfterRemoveTestRangeFromMemory(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        dao.upsert(entryAt(2));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(2), null));
        dao.upsert(entryAt(2));

        assertSame(dao.all(), entryAt(1), entryAt(2));
    }

    @DaoTest(stage = 3)
    void manyRemoveRecords(Dao<String, Entry<String>> dao) throws IOException {
        for (int i = 0; i < 6; i++) {
            dao.upsert(entryAt(i));
        }
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(1), null));
        dao.upsert(new BaseEntry<>(keyAt(2), null));
        dao.upsert(new BaseEntry<>(keyAt(4), null));
        dao.upsert(new BaseEntry<>(keyAt(5), null));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(3), null));
        dao.upsert(entryAt(4));
        dao.upsert(new BaseEntry<>(keyAt(5), "new value"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entryAt(2));
        dao.upsert(entryAt(1));

        for (Iterator<Entry<String>> it = dao.all(); it.hasNext(); ) {
            Entry<String> entry = it.next();
            System.out.println(entry);
        }

        assertSame(
                dao.all(),
                entryAt(0),
                entryAt(1),
                entryAt(2),
                entryAt(4),
                new BaseEntry<>(keyAt(5), "new value")
        );
    }

}
