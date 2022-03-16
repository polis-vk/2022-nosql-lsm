package ru.mail.polis.saskov;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;

public class PersistRangeTest extends BaseTest {
    @DaoTest(stage = 3)
    void testSingle(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("a", "b"));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(
                dao.all(),
                entry("a", "b")
        );
    }

    @DaoTest(stage = 3)
    void testTree(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("e", "f"));
        dao.upsert(entry("c", "d"));
        dao.upsert(entry("a", "b"));

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(
                dao.all(),

                entry("a", "b"),
                entry("c", "d"),
                entry("e", "f")
        );
    }

    @DaoTest(stage = 3)
    void testFindRangeInTheMiddle(Dao<String, Entry<String>> dao) throws Exception {
        dao.upsert(entry("e", "f"));
        dao.upsert(entry("c", "d"));
        dao.upsert(entry("a", "b"));

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get("c", "e"), entry("c", "d"));
    }

    @DaoTest(stage = 3)
    void testFindFullRange(Dao<String, Entry<String>> dao) throws Exception {
        dao.upsert(entry("e", "f"));
        dao.upsert(entry("c", "d"));
        dao.upsert(entry("a", "b"));

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(
                dao.get("a", "z"),

                entry("a", "b"),
                entry("c", "d"),
                entry("e", "f")
        );
    }

    @DaoTest(stage = 3)
    void testAllTo(Dao<String, Entry<String>> dao) throws Exception {
        dao.upsert(entry("e", "f"));
        dao.upsert(entry("c", "d"));
        dao.upsert(entry("a", "b"));

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(
                dao.allTo("e"),

                entry("a", "b"),
                entry("c", "d")
        );
    }
}
