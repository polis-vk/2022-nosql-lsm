package ru.mail.polis.pavelkovalenko.stage5;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractTest extends BaseTest {

    private static final int FLUSH_TRESHOLD = 1 << 20; // 1 MB
    private static final int ENTRY_SIZE = EntrySizeRealization.ONE.getEntrySize();

    protected static final int N_ENTRIES_FOR_AUTOFLUSH = (int) Math.ceil((double) FLUSH_TRESHOLD / ENTRY_SIZE);
    protected static final int N_ENTRIES_FOR_ABSENT_AUTOFLUSH = N_ENTRIES_FOR_AUTOFLUSH - 1;

    @DaoTest(stage = 5)
    void concurrentDaoCalls(Dao<String, Entry<String>> dao) throws Exception {
        int nTasks = 5; // GET, UPSERT, FLUSH, COMPACT, CLOSE
        List<ParallelTask> tasks = List.of(
                GET -> {
                    int randomPos = N_ENTRIES_FOR_AUTOFLUSH * N_ENTRIES_FOR_AUTOFLUSH;
                    Entry<String> randomEntry = entryAt(randomPos);

                    long millisElapsed = Timer.elapseMs(() -> {
                        dao.upsert(randomEntry);
                        assertSame(dao.get(keyAt(randomPos)), randomEntry);
                    });
                    assertTrue(millisElapsed < 50);

                    int count = N_ENTRIES_FOR_AUTOFLUSH / 10;
                    List<Entry<String>> entries = new ArrayList<>(count);
                    for (int i = 0; i < count; ++i) {
                        entries.add(entryAt(i));
                    }
                    entries.add(randomEntry);

                    runInParallel(100, count, value -> dao.upsert(entries.get(value))).close();

                    millisElapsed = Timer.elapseMs(() -> assertSame(dao.all(), entries));
                    assertTrue(millisElapsed < 150);

                    dao.upsert(new BaseEntry<>(keyAt(randomPos), null));
                    runInParallel(100, count,
                            value -> dao.upsert(new BaseEntry<>(entries.get(value).key(), null))).close();
                },
                UPSERT -> {
                    int randomPos = N_ENTRIES_FOR_AUTOFLUSH * N_ENTRIES_FOR_AUTOFLUSH - 1;
                    Entry<String> randomEntry = entryAt(randomPos);
                    long millisElapsed = Timer.elapseMs(() -> {
                        dao.upsert(randomEntry);
                        assertSame(randomEntry, dao.get(keyAt(randomPos)));
                    });
                    assertTrue(millisElapsed < 50);

                    int count = N_ENTRIES_FOR_ABSENT_AUTOFLUSH / 6 - N_ENTRIES_FOR_ABSENT_AUTOFLUSH / 10;
                    List<Entry<String>> entries = new ArrayList<>(count);
                    // Make set of entries in GET disjoint with set of entries in UPSERT
                    // to check of unique data and to avoid nulled entries by other threads
                    for (int i = N_ENTRIES_FOR_ABSENT_AUTOFLUSH / 10; i < N_ENTRIES_FOR_ABSENT_AUTOFLUSH / 6; ++i) {
                        entries.add(entryAt(i));
                    }
                    entries.add(randomEntry);

                    runInParallel(100, count, value -> dao.upsert(entries.get(value))).close();

                    millisElapsed = Timer.elapseMs(() -> assertSame(dao.all(), entries));
                    assertTrue(millisElapsed < 150);

                    dao.upsert(new BaseEntry<>(keyAt(randomPos), null));
                    runInParallel(100, count,
                            value -> dao.upsert(new BaseEntry<>(entries.get(value).key(), null))).close();
                },
                FLUSH -> {
                    int nIterations = 2;

                    List<Entry<String>> randomEntries = new ArrayList<>(nIterations);
                    for (int i = 1; i <= nIterations; ++i) {
                        int randomPos = N_ENTRIES_FOR_AUTOFLUSH * N_ENTRIES_FOR_ABSENT_AUTOFLUSH - i;
                        Entry<String> randomEntry = entryAt(randomPos);
                        randomEntries.add(randomEntry);

                        dao.upsert(randomEntry);
                        dao.flush();

                        Utils.assertNFilesInConfigDir(dao, 2 * i);

                        long millisElapsed = Timer.elapseMs(() -> assertSame(randomEntry, dao.get(keyAt(randomPos))));
                        assertTrue(millisElapsed < 100);
                    }

                    runInParallel(nIterations,
                            value -> dao.upsert(new BaseEntry<>(randomEntries.get(value).key(), null))).close();
                },
                COMPACT -> {
                    int count = N_ENTRIES_FOR_ABSENT_AUTOFLUSH / 4 - N_ENTRIES_FOR_ABSENT_AUTOFLUSH / 6;
                    List<Entry<String>> entries = new ArrayList<>(count);
                    // Make sets of entries in GET and UPSERT disjoint with set of entries in COMPACT
                    // to check of unique data and to avoid nulled entries by other threads
                    for (int i = N_ENTRIES_FOR_ABSENT_AUTOFLUSH / 6; i < N_ENTRIES_FOR_ABSENT_AUTOFLUSH / 4; ++i) {
                        Entry<String> curEntry;
                        if (i % 2 == 0) {
                            curEntry = entryAt(i);
                            entries.add(curEntry);
                        } else {
                            curEntry = new BaseEntry<>(keyAt(i), null);
                        }
                        dao.upsert(curEntry);

                        // There will be three flushes that will be executed later than ones in FLUSH
                        if (i % (count / 4) == 0) {
                            dao.flush();
                            Utils.assertSomeFilesInConfigDir(dao);
                        }
                    }

                    // So we can be sure that compact leaves behind exactly 2 files
                    Utils.assertCompactManyTimes(dao);
                    Utils.assertNFilesInConfigDir(dao, 2);

                    long millisElapsed = Timer.elapseMs(() -> assertSame(dao.all(), entries));
                    assertTrue(millisElapsed < 500);

                    runInParallel(100, count,
                            value -> dao.upsert(new BaseEntry<>(entries.get(value).key(), null))).close();
                },
                CLOSE -> {
                    Thread.sleep(4_000);
                    long millisElapsed = Timer.elapseMs(dao::close);
                    assertTrue(millisElapsed > 50);
                    assertTrue(millisElapsed < 500);
                    Utils.assertNFilesInConfigDir(dao, 2);
                }
        );

        ParallelTask taskController = i -> tasks.get(i).run(i);
        runInParallel(nTasks, taskController).close();

        assertSame(dao.all(), Collections.emptyList());
    }
}
