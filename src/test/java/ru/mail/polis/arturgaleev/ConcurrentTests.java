package ru.mail.polis.arturgaleev;

import ru.mail.polis.BaseTest;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.DaoTest;
import ru.mail.polis.Entry;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConcurrentTests extends BaseTest {

    @DaoTest(stage = 5)
    void checkAutoFlush(Dao<String, Entry<String>> dao) throws Exception {
        Config config = DaoFactory.Factory.extractConfig(dao);
        // На занятии говорили о том, что это число будет вмещаться в int
        int flushThresholdBytes = (int) config.flushThresholdBytes();
        int entryBytes = 22; // 'k' + 10 chars + 'v' + 10 chars
        int numberOfEntriesForFlush = (flushThresholdBytes) / entryBytes + 100;

        // Сохраняем текущий размер файлов
        long beginSizeOfFiles = sizePersistentData(config);

        List<Entry<String>> entries = entries("k", "v", numberOfEntriesForFlush);
        runInParallel(20, numberOfEntriesForFlush, value -> dao.upsert(entries.get(value))).close();

        // Ожидание автоматического flush
        Thread.sleep(100);

        // Проверка записалось ли хоть что-то на диск
        assertTrue(beginSizeOfFiles < sizePersistentData(config));
    }

    @DaoTest(stage = 5)
    void checkFlushRead(Dao<String, Entry<String>> dao) throws Exception {
        int numberOfEntries = 500;
        List<Entry<String>> entries = entries("k", "v", numberOfEntries);

        runInParallel(100, numberOfEntries, value -> {
            // Проверка читаемости данных несмотря на flush
            try {
                dao.upsert(entries.get(value));
                dao.flush();
                assertContains(dao.all(), entries.get(value));
            } catch (Exception ignored) {
                // Если возникнет переполнение очереди, то ничего страшного
            }
        }).close();
    }

    @DaoTest(stage = 5)
    void checkCompactRead(Dao<String, Entry<String>> dao) throws Exception {
        int numberOfEntries = 500;
        List<Entry<String>> entries = entries("k", "v", numberOfEntries);

        runInParallel(100, numberOfEntries, value -> {
            // Проверка читаемости данных несмотря на compact и flush
            try {
                dao.upsert(entries.get(value));
                dao.flush();
                dao.compact();
                assertContains(dao.all(), entries.get(value));
            } catch (Exception ignored) {
                // Если возникнет переполнение очереди, то ничего страшного
            }
        }).close();
    }

    @DaoTest(stage = 5)
    void checkCompactTime(Dao<String, Entry<String>> dao) throws Exception {
        int numberOfEntries = 500;
        int numberOfFiles = 5;
        List<Entry<String>> entries = entries("k", "v", numberOfEntries);

        for (int i = 0; i < numberOfFiles; i++) {
            entries.forEach(dao::upsert);
            dao.flush();
            // Ожидание flush
            Thread.sleep(100);

            // Проверка времени выполнения compact
            Instant beginTime = Instant.now();
            dao.compact();
            Instant endTime = Instant.now();
            assertTrue(Duration.between(beginTime, endTime).toMillis() < 10);
        }
    }

    @DaoTest(stage = 5)
    void checkIdempotentClose(Dao<String, Entry<String>> dao) throws Exception {
        dao.close();
        assertDoesNotThrow(dao::close);
    }

    @DaoTest(stage = 5)
    void checkFlushOverload(Dao<String, Entry<String>> dao) {
        // Данный тест корректен при условии, что autoFlush работает корректно

        // На занятии говорили о том, что это число будет вмещаться в int
        int flushThresholdBytes = (int) DaoFactory.Factory.extractConfig(dao).flushThresholdBytes();
        int entryBytes = 22; // 'k' + 10 chars + 'v' + 10 chars
        int numberOfEntriesForFlush = (flushThresholdBytes) / entryBytes + 100;
        int numberOfFlushes = 4;
        int count = numberOfEntriesForFlush * numberOfFlushes;
        List<Entry<String>> entries = entries("k", "v", count);

        // Апсертим большое количество entry, которое должно вызвать переполнение очереди на запись
        assertThrows(Exception.class,
                runInParallel(20, count, value -> dao.upsert(entries.get(value)))::close);
    }

    @DaoTest(stage = 5)
    void checkCompactOnlyFiles(Dao<String, Entry<String>> dao) throws IOException, InterruptedException {
        int numberOfEntries = 50;
        List<Entry<String>> entries = entries("k", "v", numberOfEntries);
        entries.forEach(dao::upsert);

        dao.flush();
        // Ожидание flush
        Thread.sleep(100);

        // Замеряем сколько байт содержится
        long beginSize = sizePersistentData(dao);

        entries.forEach(entry -> dao.upsert(entry(entry.key(), null)));
        dao.compact();
        // Ожидание compact
        Thread.sleep(100);

        // Если compact использует память при сжатии, то новый файл будет пуст
        assertEquals(beginSize, sizePersistentData(dao));
    }
}
