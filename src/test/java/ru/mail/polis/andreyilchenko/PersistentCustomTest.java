package ru.mail.polis.andreyilchenko;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;

public class PersistentCustomTest extends BaseTest {

    @DaoTest(stage = 2)
    void firstEntry(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("a", "b"));
        dao.upsert(entry("bKey!", "VALUE"));
        dao.upsert(entry("ccadcacwkka", "d"));
        dao.upsert(entry("ergafe", "faw54fa5w4fw35a"));
        dao.upsert(entry("ewakfakorw", "1"));
        dao.upsert(entry("fff", "v1"));
        dao.upsert(entry("nawfjf", "afl"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get("a"), entry("a", "b"));
    }

    @DaoTest(stage = 2)
    void secondEntry(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("a", "b"));
        dao.upsert(entry("bKey!", "VALUE"));
        dao.upsert(entry("ccadcacwkka", "d"));
        dao.upsert(entry("ergafe", "faw54fa5w4fw35a"));
        dao.upsert(entry("ewakfakorw", "1"));
        dao.upsert(entry("fff", "v1"));
        dao.upsert(entry("nawfjf", "afl"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get("bKey!"), entry("bKey!", "VALUE"));
    }

    @DaoTest(stage = 2)
    void middleEntry(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("a", "b"));
        dao.upsert(entry("bKey!", "VALUE"));
        dao.upsert(entry("ccadcacwkka", "d"));
        dao.upsert(entry("ewak12fakorw", "1"));
        dao.upsert(entry("fttt", "faw54fa5w4fw35a"));
        dao.upsert(entry("fff", "v1"));
        dao.upsert(entry("nawfjf", "afl"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get("ewak12fakorw"), entry("ewak12fakorw", "1"));
    }

    @DaoTest(stage = 2)
    void preLastEntry(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("a", "b"));
        dao.upsert(entry("afakfjwakjf", "awfawffw"));
        dao.upsert(entry("bbbfawjoawwfa", "kfkaafiwjkwfk"));
        dao.upsert(entry("ccadcacwkka", "d"));
        dao.upsert(entry("ergafe", "faw54fa5w4fw35a"));
        dao.upsert(entry("ewakfakorw", "1"));
        dao.upsert(entry("fff", "v1"));
        dao.upsert(entry("nawfjf", "afl"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get("fff"), entry("fff", "v1"));
    }

    @DaoTest(stage = 2)
    void lastEntry(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("a", "b"));
        dao.upsert(entry("afakfjwakjf", "awfawffw"));
        dao.upsert(entry("bbbfawjoawwfa", "kfkaafiwjkwfk"));
        dao.upsert(entry("ccadcacwkka", "d"));
        dao.upsert(entry("ergafe", "faw54fa5w4fw35a"));
        dao.upsert(entry("ewakfakorw", "1"));
        dao.upsert(entry("fff", "v1"));
        dao.upsert(entry("nawfjf", "afl"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get("nawfjf"), entry("nawfjf", "afl"));
    }


}
