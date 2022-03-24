package ru.mail.polis.lutsenkodmitrii;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class Stage3Test extends BaseTest {

    private static final int[] DATASET = new int[]{1, 4, 5, 6, 8, 11, 15, 20, 100};


    @DaoTest(stage = 3)
    void from1(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(5));

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.allFrom(keyAt(5)), entryAt(5));
    }

    @DaoTest(stage = 3)
    void from2LongValue(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entryAt(5));
        String longString = "z".repeat(200);
        dao.upsert(entry(keyAt(46), longString));

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        Iterator<Entry<String>> entryIterator = dao.allFrom(keyAt(5));
        assertSame(entryIterator,
                entryAt(5),
                entry(keyAt(46), longString)
        );
        assertEmpty(entryIterator);
    }

    @DaoTest(stage = 3)
    void fromSimple(Dao<String, Entry<String>> dao) throws IOException {
        for (int i : DATASET) {
            dao.upsert(entryAt(i));
        }

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.allFrom(keyAt(14)), allFrom(14, DATASET));
        assertSame(dao.allFrom(keyAt(16)), allFrom(16, DATASET));
    }

    @DaoTest(stage = 3)
    void getAll(Dao<String, Entry<String>> dao) throws IOException {
        dao = upsertMixingDataset(dao);
        assertSame(dao.all(), DATASET);
    }

    @DaoTest(stage = 3)
    void getFrom(Dao<String, Entry<String>> dao) throws IOException {
        dao = upsertMixingDataset(dao);
        assertSame(dao.allFrom(keyAt(7)), allFrom(7, DATASET));
        assertSame(dao.allFrom(keyAt(8)), allFrom(8, DATASET));
    }

    @DaoTest(stage = 3)
    void getTo(Dao<String, Entry<String>> dao) throws IOException {
        dao = upsertMixingDataset(dao);
        assertSame(dao.allTo(keyAt(7)), allTo(7, DATASET));
        assertSame(dao.allTo(keyAt(8)), allTo(8, DATASET));
    }

    @DaoTest(stage = 3)
    void getFromTo(Dao<String, Entry<String>> dao) throws IOException {
        dao = upsertMixingDataset(dao);
        assertSame(dao.get(keyAt(7), keyAt(16)), allFromTo(7, 16, DATASET));
    }


    @DaoTest(stage = 3)
    void emptyCloses(Dao<String, Entry<String>> dao) throws IOException {
        dao.close();
        DaoFactory.Factory.reopen(dao);
        dao.close();
        DaoFactory.Factory.reopen(dao);
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        assertEmpty(dao.all());
        assertEmpty(dao.allTo(keyAt(1)));
        assertEmpty(dao.allFrom(keyAt(1)));
        assertEmpty(dao.get(keyAt(1), keyAt(1)));
    }

    private static int[] allFrom(int from, int[] array) {
        return Arrays.stream(array).filter(value -> value >= from).toArray();
    }

    private static int[] allTo(int to, int[] array) {
        return Arrays.stream(array).filter(value -> value < to).toArray();
    }

    private static int[] allFromTo(int from, int to, int[] array) {
        return Arrays.stream(array).filter(value -> (value >= from && value < to)).toArray();
    }


    private Dao<String, Entry<String>> upsertMixingDataset(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry(keyAt(5), valueAt(555)));
        dao.upsert(entry(keyAt(11), valueAt(111)));
        dao.upsert(entry(keyAt(15), valueAt(155)));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entry(keyAt(5), valueAt(10)));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entryAt(1));
        dao.upsert(entryAt(4));
        dao.upsert(entryAt(6));
        dao.upsert(entry(keyAt(8), valueAt(888)));
        dao.upsert(entry(keyAt(11), valueAt(1111)));
        dao.upsert(entryAt(15));
        dao.upsert(entryAt(20));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(entryAt(5));
        dao.upsert(entryAt(8));
        dao.upsert(entryAt(11));
        dao.upsert(entryAt(100));
        return dao;
    }
}
