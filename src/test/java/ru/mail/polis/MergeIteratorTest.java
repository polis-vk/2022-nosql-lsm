package ru.mail.polis;

import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.util.Iterator;

public class MergeIteratorTest extends BaseTest {

    @DaoTest(stage = 3)
    void persistent(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        dao.upsert(entryAt(2));
        dao.upsert(entryAt(4));
        dao.upsert(entryAt(6));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        Iterator<Entry<String>> it = dao.get(keyAt(2), keyAt(4));
        assertSame(it.next(), entryAt(2));
        assertSame(it.next(), entryAt(4));
    }
}
