package ru.mail.polis.fetisovvladislav;

import org.junit.jupiter.api.Assertions;
import ru.mail.polis.BaseTest;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;
import ru.mail.polis.test.DaoFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static ru.mail.polis.vladislavfetisov.wal.WAL.LOG_DIR;

public class WALTest extends BaseTest {

    /**
     * Записываем данные в базу(не должно быть flush)->эмулируем выключение света->
     * поднимаем базу -> проверяем, что все значения на месте
     */
    @DaoTest(stage = 1000)
    void durabilityTest(Dao<String, Entry<String>> dao) throws Exception {
        int count = 2_500;
        List<Entry<String>> entries = entries("k", "v", count);
        boolean[] commits = new boolean[count];
        Dao<String, Entry<String>> finalDao = dao;
        try {
            runInParallel(100, count, value -> {
                finalDao.upsert(entries.get(value));
                assertContains(finalDao.all(), entries.get(value));
                if (value == 500) {
                    Thread.currentThread().interrupt();
                }
                commits[value] = true;

            }).close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        dao = DaoFactory.Factory.turnOffLightAndReopen(dao);
        for (int i = 0; i < commits.length; i++) {
            if (commits[i]) {
                assertContains(dao.all(), entries.get(i));
            }
        }
    }

    /**
     * Пишем в базу-> флашим -> проверяем, что из файлов лога только 2 новых(пустых) файла.
     */
    @DaoTest(stage = 1000)
    void redundantFilesTest(Dao<String, Entry<String>> dao) throws Exception {
        int count = 10_000;
        List<Entry<String>> entries = entries("k", "v", count);
        runInParallel(1000, count, value -> {
            dao.upsert(entries.get(value));
            assertContains(dao.all(), entries.get(value));
        }).close();

        dao.flush();
        Path basePath = DaoFactory.Factory.extractConfig(dao).basePath();
        Path logPath = basePath.resolve(LOG_DIR);
        assertTrue(Files.exists(logPath));
        try (Stream<Path> files = Files.list(logPath)) {
            assertEquals(2, files.count());
        }
        assertSame(dao.all(), entries);
    }

}
