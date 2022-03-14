package ru.mail.polis;

import org.junit.jupiter.api.Assertions;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;

public class FetisovVladislavTest extends BaseTest {
    @DaoTest(stage = 2)
    void manyRecords(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("k1", "v1"));
        dao.upsert(entry("k10", "v10"));
        dao.upsert(entry("k11", "v11"));
        dao.upsert(entry("k2", "v2"));
        dao.upsert(entry("k13", "v13"));
        dao.upsert(entry("k14", "v14"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get("k1"), entry("k1", "v1"));
        assertSame(dao.get("k10"), entry("k10", "v10"));
        assertSame(dao.get("k11"), entry("k11", "v11"));
        assertSame(dao.get("k2"), entry("k2", "v2"));
        assertSame(dao.get("k14"), entry("k14", "v14"));
        assertSame(dao.get("k13"), entry("k13", "v13"));
        Assertions.assertNull(dao.get("k25"));
        Assertions.assertNull(dao.get("k0"));

    }
}
