package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;

public class DeleteTest extends BaseTest {

    public static final int[] NOTHING = new int[0];
    private static final int[] DATASET = new int[]{7, 97, 101};
    private static final int FIRST = DATASET[0];
    private static final int LAST = DATASET[DATASET.length - 1];

    private void sliceAndDice(Dao<String, Entry<String>> dao) throws IOException {
        // Full
        assertSame(dao.all(), DATASET);

        // From
        assertSame(dao.allFrom(keyAt(6)), DATASET);
        assertSame(dao.allFrom(keyAt(7)), DATASET);
        assertSame(dao.allFrom(keyAt(8)), 97, 101);
        assertSame(dao.allFrom(keyAt(96)), 97, 101);
        assertSame(dao.allFrom(keyAt(97)), 97, 101);
        assertSame(dao.allFrom(keyAt(98)), 101);
        assertSame(dao.allFrom(keyAt(100)), 101);
        assertSame(dao.allFrom(keyAt(101)), 101);
        assertSame(dao.allFrom(keyAt(102)), NOTHING);

        // Right
        assertSame(dao.allTo(keyAt(102)), DATASET);
        assertSame(dao.allTo(keyAt(101)), 7, 97);
        assertSame(dao.allTo(keyAt(100)), 7, 97);
        assertSame(dao.allTo(keyAt(98)), 7, 97);
        assertSame(dao.allTo(keyAt(97)), 7);
        assertSame(dao.allTo(keyAt(96)), 7);
        assertSame(dao.allTo(keyAt(8)), 7);
        assertSame(dao.allTo(keyAt(7)), NOTHING);
        assertSame(dao.allTo(keyAt(6)), NOTHING);

        // Between

        assertSame(dao.get(keyAt(6), keyAt(102)), DATASET);
        assertSame(dao.get(keyAt(7), keyAt(102)), DATASET);

        assertSame(dao.get(keyAt(6), keyAt(101)), 7, 97);
        assertSame(dao.get(keyAt(7), keyAt(101)), 7, 97);
        assertSame(dao.get(keyAt(7), keyAt(98)), 7, 97);

        assertSame(dao.get(keyAt(7), keyAt(97)), 7);
        assertSame(dao.get(keyAt(6), keyAt(97)), 7);
        assertSame(dao.get(keyAt(6), keyAt(96)), 7);
        assertSame(dao.get(keyAt(6), keyAt(8)), 7);
        assertSame(dao.get(keyAt(7), keyAt(7)), NOTHING);

        assertSame(dao.get(keyAt(97), keyAt(102)), 97, 101);
        assertSame(dao.get(keyAt(96), keyAt(102)), 97, 101);
        assertSame(dao.get(keyAt(98), keyAt(102)), 101);
        assertSame(dao.get(keyAt(98), keyAt(101)), NOTHING);
        assertSame(dao.get(keyAt(98), keyAt(100)), NOTHING);
        assertSame(dao.get(keyAt(102), keyAt(1000)), NOTHING);
        assertSame(dao.get(keyAt(0), keyAt(7)), NOTHING);
        assertSame(dao.get(keyAt(0), keyAt(6)), NOTHING);
    }

    private void checkEmpty(Dao<String, Entry<String>> dao) throws IOException {
        assertEmpty(dao.all());
        assertEmpty(dao.allFrom(keyAt(FIRST)));
        assertEmpty(dao.allFrom(keyAt(LAST)));
        assertEmpty(dao.allTo(keyAt(FIRST)));
        assertEmpty(dao.allTo(keyAt(LAST)));
        assertEmpty(dao.get(keyAt(FIRST), keyAt(LAST)));
    }

    @DaoTest(stage = 3)
    void memoryOnlyNonExisting(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry(keyAt(1), null));
        checkEmpty(dao);
    }

    @DaoTest(stage = 3)
    void memoryOnlyExisting(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(FIRST));
        dao.upsert(entryAt(LAST));
        dao.upsert(entry(keyAt(FIRST), null));
        dao.upsert(entry(keyAt(LAST), null));
        checkEmpty(dao);
    }

    @DaoTest(stage = 3)
    void memoryOnlyMixed(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(DATASET[0]));
        dao.upsert(entryAt(DATASET[1]));
        dao.upsert(entryAt(DATASET[2]));
        sliceAndDice(dao);
        dao.upsert(entryAt(100));
        dao.upsert(entry(keyAt(100), null));
        sliceAndDice(dao);
        dao.upsert(entry(keyAt(DATASET[1]), null));
        dao.upsert(entryAt(DATASET[1]));
        sliceAndDice(dao);
        dao.upsert(entry(keyAt(DATASET[0]), null));
        dao.upsert(entry(keyAt(DATASET[1]), null));
        assertSame(dao.all(), LAST);
        dao.upsert(entry(keyAt(DATASET[2]), null));
        checkEmpty(dao);
    }

    @DaoTest(stage = 3)
    void diskOnlyNonExisting(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry(keyAt(1), null));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        checkEmpty(dao);
    }

    @DaoTest(stage = 3)
    void diskOnlyExisting(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(FIRST));
        dao.upsert(entryAt(LAST));
        dao.upsert(entry(keyAt(FIRST), null));
        dao.upsert(entry(keyAt(LAST), null));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        checkEmpty(dao);
    }

    @DaoTest(stage = 3)
    void diskMixed(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(DATASET[0]));
        dao.upsert(entryAt(DATASET[1]));
        dao.upsert(entryAt(DATASET[2]));
        dao.upsert(entryAt(100));
        dao.upsert(entry(keyAt(100), null));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        sliceAndDice(dao);
        /*dao.upsert(entry(keyAt(DATASET[1]), null));
        dao.upsert(entryAt(DATASET[1]));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        sliceAndDice(dao);
        dao.upsert(entry(keyAt(DATASET[0]), null));
        dao.upsert(entry(keyAt(DATASET[1]), null));
        dao.upsert(entry(keyAt(DATASET[2]), null));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        checkEmpty(dao);*/
    }
}
