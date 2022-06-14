package ru.mail.polis.fetisovvladislav;

import ru.mail.polis.*;
import ru.mail.polis.test.DaoFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.mail.polis.vladislavfetisov.wal.WAL.LOG_DIR;

public class WALTest extends BaseTest {
    private static final Random random = new Random();

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
        } catch (Exception ignored) {
        }
        dao = DaoFactory.Factory.turnOffLightAndReopen(dao);
        for (int i = 0; i < commits.length; i++) {
            if (commits[i]) {
                assertContains(dao.all(), entries.get(i));
            }
        }
    }

    /**
     * Пишем в базу много больших одинаковых значений
     * -> флашим -> проверяем, что из файлов лога только 2 новых(пустых) файла.
     */
    @DaoTest(stage = 1000)
    void redundantFilesTest(Dao<String, Entry<String>> dao) throws Exception {
        int count = 4_000;
        String key = withSuffix("k_", 1000);
        String value = withSuffix("v_", 1000);
        Entry<String> bigEntry = new BaseEntry<>(key, value);
        runInParallel(300, count, index -> {
            dao.upsert(bigEntry);
            assertContains(dao.all(), bigEntry);
        }).close();

        dao.flush();
        Path basePath = DaoFactory.Factory.extractConfig(dao).basePath();
        Path logPath = basePath.resolve(LOG_DIR);
        assertTrue(Files.exists(logPath));
        try (Stream<Path> files = Files.list(logPath)) {
            assertEquals(2, files.count());
        }
        assertSame(dao.all(), bigEntry);

    }

    private static String withSuffix(String value, int length) {
        byte[] initialChars = value.getBytes();
        byte[] result = new byte[initialChars.length + length];
        System.arraycopy(initialChars, 0, result, 0, initialChars.length);
        for (int i = initialChars.length; i < result.length; i++) {
            result[i] = (byte) ('A' + random.nextInt(26));
        }
        return new String(result);
    }
}
