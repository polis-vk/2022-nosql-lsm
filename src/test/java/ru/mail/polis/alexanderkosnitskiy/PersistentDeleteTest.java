package ru.mail.polis.alexanderkosnitskiy;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PersistentDeleteTest extends BaseTest {
    
    @DaoTest(stage = 3)
    void deleteOldValue(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry(keyAt(1), "removable"));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        dao.upsert(entry(keyAt(1), null));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        assertNull(dao.get(keyAt(1)));
        assertFalse(dao.get(keyAt(1), null).hasNext());
    }

    @DaoTest(stage = 3)
    void deleteFromMemory(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry(keyAt(1), "removable"));
        dao.upsert(entry(keyAt(1), null));

        assertNull(dao.get(keyAt(1)));
        assertFalse(dao.get(keyAt(1), null).hasNext());
    }

    @DaoTest(stage = 3)
    void emptyValueStringIsNotDeleted(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(7));
        dao.upsert(entry(keyAt(1), ""));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        assertSame(dao.get(keyAt(1)), entry(keyAt(1), ""));
    }

    @DaoTest(stage = 3)
    void CheckFlow(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(1));
        dao.upsert(entryAt(2));
        dao.upsert(entryAt(3));
        dao.upsert(entryAt(4));

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry(keyAt(2), null));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        Iterator<Entry<String>> iter = dao.get(null, null);

        assertSame(iter.next(), entryAt(1));
        assertSame(iter.next(), entryAt(3));
        assertSame(iter.next(), entryAt(4));
        assertFalse(iter.hasNext());
    }
}
