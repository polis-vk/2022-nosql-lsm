package ru.mail.polis;

import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Stage3Test extends BaseTest {

    @DaoTest(stage = 3)
    void from1(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(new BaseEntry<>(keyAt(5), valueAt(5)));

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get(keyAt(5), null).next(), entryAt(5));
    }

    @DaoTest(stage = 3)
    void from2LongValue(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(new BaseEntry<>(keyAt(5), valueAt(5)));
        String longString = "z".repeat(200);
        dao.upsert(new BaseEntry<>(keyAt(46), longString));

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        Iterator<Entry<String>> entryIterator = dao.get(keyAt(5), null);
        assertSame(entryIterator.next(), entryAt(5));
        assertSame(entryIterator.next(), new BaseEntry<>(keyAt(46), longString));
    }

    @DaoTest(stage = 3)
    void fromSimple(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(new BaseEntry<>(keyAt(5), valueAt(5)));
        dao.upsert(new BaseEntry<>(keyAt(8), valueAt(8)));
        dao.upsert(new BaseEntry<>(keyAt(11), valueAt(11)));
        dao.upsert(new BaseEntry<>(keyAt(12), valueAt(12)));
        dao.upsert(new BaseEntry<>(keyAt(16), valueAt(16)));
        dao.upsert(new BaseEntry<>(keyAt(19), valueAt(19)));
        dao.upsert(new BaseEntry<>(keyAt(25), valueAt(25)));
        dao.upsert(new BaseEntry<>(keyAt(27), valueAt(27)));
        dao.upsert(new BaseEntry<>(keyAt(29), valueAt(29)));
        dao.upsert(new BaseEntry<>(keyAt(35), valueAt(35)));
        dao.upsert(new BaseEntry<>(keyAt(39), valueAt(39)));
        dao.upsert(new BaseEntry<>(keyAt(41), valueAt(41)));
        String longString = "z".repeat(250);
        dao.upsert(new BaseEntry<>(keyAt(46), longString));

        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        List<BaseEntry<String>> expectedEntriesList = List.of(
                new BaseEntry<>(keyAt(16), valueAt(16)),
                new BaseEntry<>(keyAt(19), valueAt(19)),
                new BaseEntry<>(keyAt(25), valueAt(25)),
                new BaseEntry<>(keyAt(27), valueAt(27)),
                new BaseEntry<>(keyAt(29), valueAt(29)),
                new BaseEntry<>(keyAt(35), valueAt(35)),
                new BaseEntry<>(keyAt(39), valueAt(39)),
                new BaseEntry<>(keyAt(41), valueAt(41)),
                new BaseEntry<>(keyAt(46), longString)
        );
        Iterator<Entry<String>> entryIteratorFromEqualsFirstKey = dao.get(keyAt(16), null);
        expectedEntriesList.forEach(entry -> assertSame(entryIteratorFromEqualsFirstKey.next(), entry));
        Iterator<Entry<String>> entryIteratorFromDiffFirstKey = dao.get(keyAt(14), null);
        expectedEntriesList.forEach(entry -> assertSame(entryIteratorFromDiffFirstKey.next(), entry));
        assertEmpty(entryIteratorFromEqualsFirstKey);
        assertEmpty(entryIteratorFromDiffFirstKey);
    }

    @DaoTest(stage = 3)
    void getAll(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(new BaseEntry<>(keyAt(5), valueAt(5)));
        dao.upsert(new BaseEntry<>(keyAt(11), valueAt(111)));
        dao.upsert(new BaseEntry<>(keyAt(15), valueAt(15)));
        dao.upsert(new BaseEntry<>(keyAt(22), valueAt(22)));
        dao.upsert(new BaseEntry<>(keyAt(25), valueAt(25)));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(5), valueAt(10)));
        dao.upsert(new BaseEntry<>(keyAt(22), valueAt(222)));
        dao.upsert(new BaseEntry<>(keyAt(25), valueAt(25)));
        dao.upsert(new BaseEntry<>(keyAt(27), valueAt(27)));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(1), valueAt(1)));
        dao.upsert(new BaseEntry<>(keyAt(4), valueAt(4)));
        dao.upsert(new BaseEntry<>(keyAt(6), valueAt(6)));
        dao.upsert(new BaseEntry<>(keyAt(11), valueAt(1111)));
        dao.upsert(new BaseEntry<>(keyAt(20), valueAt(20)));
        dao.upsert(new BaseEntry<>(keyAt(25), valueAt(525)));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(5), valueAt(555)));
        dao.upsert(new BaseEntry<>(keyAt(8), valueAt(888)));
        dao.upsert(new BaseEntry<>(keyAt(11), valueAt(11)));
        dao.upsert(new BaseEntry<>(keyAt(100), valueAt(100)));

        List<BaseEntry<String>> expectedEntriesList = List.of(
                new BaseEntry<>(keyAt(1), valueAt(1)),
                new BaseEntry<>(keyAt(4), valueAt(4)),
                new BaseEntry<>(keyAt(5), valueAt(555)),
                new BaseEntry<>(keyAt(6), valueAt(6)),
                new BaseEntry<>(keyAt(8), valueAt(888)),
                new BaseEntry<>(keyAt(11), valueAt(11)),
                new BaseEntry<>(keyAt(15), valueAt(15)),
                new BaseEntry<>(keyAt(20), valueAt(20)),
                new BaseEntry<>(keyAt(22), valueAt(222)),
                new BaseEntry<>(keyAt(25), valueAt(525)),
                new BaseEntry<>(keyAt(27), valueAt(27)),
                new BaseEntry<>(keyAt(100), valueAt(100))
        );
        Iterator<Entry<String>> allIterator = dao.get(null, null);
        expectedEntriesList.forEach(entry -> assertSame(allIterator.next(), entry));
        assertEmpty(allIterator);
    }

    @DaoTest(stage = 3)
    void getFrom(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(new BaseEntry<>(keyAt(5), valueAt(5)));
        dao.upsert(new BaseEntry<>(keyAt(11), valueAt(111)));
        dao.upsert(new BaseEntry<>(keyAt(15), valueAt(15)));
        dao.upsert(new BaseEntry<>(keyAt(22), valueAt(22)));
        dao.upsert(new BaseEntry<>(keyAt(25), valueAt(25)));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(5), valueAt(10)));
        dao.upsert(new BaseEntry<>(keyAt(22), valueAt(222)));
        dao.upsert(new BaseEntry<>(keyAt(25), valueAt(25)));
        dao.upsert(new BaseEntry<>(keyAt(27), valueAt(27)));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(1), valueAt(1)));
        dao.upsert(new BaseEntry<>(keyAt(4), valueAt(4)));
        dao.upsert(new BaseEntry<>(keyAt(6), valueAt(6)));
        dao.upsert(new BaseEntry<>(keyAt(11), valueAt(1111)));
        dao.upsert(new BaseEntry<>(keyAt(20), valueAt(20)));
        dao.upsert(new BaseEntry<>(keyAt(25), valueAt(525)));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(5), valueAt(555)));
        dao.upsert(new BaseEntry<>(keyAt(8), valueAt(888)));
        dao.upsert(new BaseEntry<>(keyAt(11), valueAt(11)));
        dao.upsert(new BaseEntry<>(keyAt(100), valueAt(100)));

        List<BaseEntry<String>> expectedEntriesList = List.of(
                new BaseEntry<>(keyAt(8), valueAt(888)),
                new BaseEntry<>(keyAt(11), valueAt(11)),
                new BaseEntry<>(keyAt(15), valueAt(15)),
                new BaseEntry<>(keyAt(20), valueAt(20)),
                new BaseEntry<>(keyAt(22), valueAt(222)),
                new BaseEntry<>(keyAt(25), valueAt(525)),
                new BaseEntry<>(keyAt(27), valueAt(27)),
                new BaseEntry<>(keyAt(100), valueAt(100))
        );
        Iterator<Entry<String>> entryIteratorFromEqualsLastKey = dao.get(keyAt(8), null);
        expectedEntriesList.forEach(entry -> assertSame(entryIteratorFromEqualsLastKey.next(), entry));
        Iterator<Entry<String>> entryIteratorFromDiffLastKey = dao.get(keyAt(7), null);
        expectedEntriesList.forEach(entry -> assertSame(entryIteratorFromDiffLastKey.next(), entry));
    }

    @DaoTest(stage = 3)
    void getTo(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(new BaseEntry<>(keyAt(5), valueAt(5)));
        dao.upsert(new BaseEntry<>(keyAt(11), valueAt(111)));
        dao.upsert(new BaseEntry<>(keyAt(15), valueAt(15)));
        dao.upsert(new BaseEntry<>(keyAt(22), valueAt(22)));
        dao.upsert(new BaseEntry<>(keyAt(25), valueAt(25)));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(5), valueAt(10)));
        dao.upsert(new BaseEntry<>(keyAt(22), valueAt(222)));
        dao.upsert(new BaseEntry<>(keyAt(25), valueAt(25)));
        dao.upsert(new BaseEntry<>(keyAt(27), valueAt(27)));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(1), valueAt(1)));
        dao.upsert(new BaseEntry<>(keyAt(4), valueAt(4)));
        dao.upsert(new BaseEntry<>(keyAt(6), valueAt(6)));
        dao.upsert(new BaseEntry<>(keyAt(11), valueAt(1111)));
        dao.upsert(new BaseEntry<>(keyAt(20), valueAt(20)));
        dao.upsert(new BaseEntry<>(keyAt(25), valueAt(525)));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(5), valueAt(555)));
        dao.upsert(new BaseEntry<>(keyAt(8), valueAt(888)));
        dao.upsert(new BaseEntry<>(keyAt(11), valueAt(11)));
        dao.upsert(new BaseEntry<>(keyAt(100), valueAt(100)));

        List<BaseEntry<String>> expectedEntriesList = List.of(
                new BaseEntry<>(keyAt(1), valueAt(1)),
                new BaseEntry<>(keyAt(4), valueAt(4)),
                new BaseEntry<>(keyAt(5), valueAt(555)),
                new BaseEntry<>(keyAt(6), valueAt(6))
        );
        Iterator<Entry<String>> entryIteratorFromEqualsFirsKey = dao.get(null, keyAt(8));
        expectedEntriesList.forEach(entry -> assertSame(entryIteratorFromEqualsFirsKey.next(), entry));
        Iterator<Entry<String>> entryIteratorFromDiffFirsKey = dao.get(null, keyAt(7));
        expectedEntriesList.forEach(entry -> assertSame(entryIteratorFromDiffFirsKey.next(), entry));
        assertEmpty(entryIteratorFromEqualsFirsKey);
        assertEmpty(entryIteratorFromDiffFirsKey);
    }

    @DaoTest(stage = 3)
    void getFromTo(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(new BaseEntry<>(keyAt(5), valueAt(5)));
        dao.upsert(new BaseEntry<>(keyAt(11), valueAt(111)));
        dao.upsert(new BaseEntry<>(keyAt(15), valueAt(15)));
        dao.upsert(new BaseEntry<>(keyAt(22), valueAt(22)));
        dao.upsert(new BaseEntry<>(keyAt(25), valueAt(25)));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(5), valueAt(10)));
        dao.upsert(new BaseEntry<>(keyAt(22), valueAt(222)));
        dao.upsert(new BaseEntry<>(keyAt(25), valueAt(25)));
        dao.upsert(new BaseEntry<>(keyAt(27), valueAt(27)));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(1), valueAt(1)));
        dao.upsert(new BaseEntry<>(keyAt(4), valueAt(4)));
        dao.upsert(new BaseEntry<>(keyAt(6), valueAt(6)));
        dao.upsert(new BaseEntry<>(keyAt(11), valueAt(1111)));
        dao.upsert(new BaseEntry<>(keyAt(20), valueAt(20)));
        dao.upsert(new BaseEntry<>(keyAt(25), valueAt(525)));
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        dao.upsert(new BaseEntry<>(keyAt(5), valueAt(555)));
        dao.upsert(new BaseEntry<>(keyAt(8), valueAt(888)));
        dao.upsert(new BaseEntry<>(keyAt(11), valueAt(11)));
        dao.upsert(new BaseEntry<>(keyAt(100), valueAt(100)));

        List<BaseEntry<String>> expectedEntriesList = List.of(
                new BaseEntry<>(keyAt(6), valueAt(6)),
                new BaseEntry<>(keyAt(8), valueAt(888)),
                new BaseEntry<>(keyAt(11), valueAt(11)),
                new BaseEntry<>(keyAt(15), valueAt(15)),
                new BaseEntry<>(keyAt(20), valueAt(20)),
                new BaseEntry<>(keyAt(22), valueAt(222))
        );
        Iterator<Entry<String>> entryIteratorFromTo = dao.get(keyAt(6), keyAt(24));
        expectedEntriesList.forEach(entry -> assertSame(entryIteratorFromTo.next(), entry));
        assertEmpty(entryIteratorFromTo);
    }


    @DaoTest(stage = 3)
    void getWithMemoryPressure(Dao<String, Entry<String>> dao) throws IOException {
        int keys1 = 75_000;
        int keys2 = 100_000;
        int entityIndex1 = keys1 / 2 - 7;
        int entityIndex2 = keys2 / 2 - 7;
        List<Entry<String>> entries1 = entries(keys1);
        List<Entry<String>> entries2 = entries(keys2);
        List<Entry<String>> tmp = new ArrayList<>(entries1);
        tmp.addAll(entries2);

        entries1.forEach(dao::upsert);
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        entries2.forEach(dao::upsert);
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        entries1.forEach(dao::upsert);
        dao.close();

        assertValueAt(DaoFactory.Factory.reopen(dao), entityIndex1);
        dao.close();
        assertValueAt(DaoFactory.Factory.reopen(dao), entityIndex2);

        assertSame(
                tmp.get(entityIndex1),
                entries1.get(entityIndex1)
        );
        assertSame(
                tmp.get(entityIndex2),
                entries2.get(entityIndex2)
        );
    }

    @DaoTest(stage = 3)
    void emptyCloses(Dao<String, Entry<String>> dao) throws IOException {
        dao.close();
        DaoFactory.Factory.reopen(dao);
        dao.close();
        DaoFactory.Factory.reopen(dao);
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);
        assertEmpty(dao.get(null, null));
        assertEmpty(dao.get(null, keyAt(1)));
        assertEmpty(dao.get(keyAt(1), null));
        assertEmpty(dao.get(keyAt(1), keyAt(1)));
    }

}
