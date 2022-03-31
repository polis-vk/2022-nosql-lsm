package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;

public class MultiLineAndEscapedTest extends BaseTest {

    @DaoTest(stage = 2)
    void escapedMultiLine1(Dao<String, Entry<String>> dao) throws IOException {
        final Entry<String> entry = entry("key1\\\nkey2", "value1\\nvalue2");
        dao.upsert(entry);
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get(entry.key()), entry);
    }

    @DaoTest(stage = 2)
    void escapedMultiLine2(Dao<String, Entry<String>> dao) throws IOException {
        final Entry<String> entry = entry("key1\\\\nkey2", "value1\\nvalue2");
        dao.upsert(entry);
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get(entry.key()), entry);
    }

    @DaoTest(stage = 2)
    void escapedMultiLine3(Dao<String, Entry<String>> dao) throws IOException {
        final Entry<String> entry = entry("key1\\\\\nkey2", "value1\\nvalue2");
        dao.upsert(entry);
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get(entry.key()), entry);
    }

    @DaoTest(stage = 2)
    void escapedMultiLine4(Dao<String, Entry<String>> dao) throws IOException {
        final Entry<String> entry = entry("\\\\\nkey1\nk\\ne\\\\ny2\n", "value1\\nvalue2");
        dao.upsert(entry);
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get(entry.key()), entry);
    }

    @DaoTest(stage = 2)
    void escapedMultiLine5(Dao<String, Entry<String>> dao) throws IOException {
        final Entry<String> entry = entry("\\\\\nke\\y1\nk\\ne\\\\n\\\\y2\n", "val\\nu\n\n\ne1\\nva\\\nlu\\\\e\\\\\\2");
        dao.upsert(entry);
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get(entry.key()), entry);
    }
}
