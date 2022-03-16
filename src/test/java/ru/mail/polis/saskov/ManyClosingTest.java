package ru.mail.polis.saskov;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;

public class ManyClosingTest extends BaseTest {
    @DaoTest(stage = 3)
    void testSingle(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("a", "b"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry("b", "c"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        assertSame(
                dao.all(),

                entry("a", "b"),
                entry("b", "c")
        );
    }

    @DaoTest(stage = 3)
    void testTree(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("e", "f"));
        dao.upsert(entry("c", "d"));
        dao.upsert(entry("a", "b"));

        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry("k", "l"));
        dao.upsert(entry("i", "j"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry("g", "h"));
        assertSame(
                dao.all(),

                entry("a", "b"),
                entry("c", "d"),
                entry("e", "f"),
                entry("g", "h"),
                entry("i", "j"),
                entry("k", "l")
        );
    }

    @DaoTest(stage = 3)
    void testFindRangeInTheMiddle(Dao<String, Entry<String>> dao) throws Exception {
        dao.upsert(entry("e", "f"));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry("c", "d"));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry("a", "b"));

        assertSame(dao.get("c", "e"), entry("c", "d"));
    }

    @DaoTest(stage = 3)
    void testFindFullRange(Dao<String, Entry<String>> dao) throws Exception {
        dao.upsert(entry("e", "f"));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry("c", "d"));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry("a", "b"));

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
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
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
