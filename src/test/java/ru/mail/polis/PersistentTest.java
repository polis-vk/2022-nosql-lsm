package ru.mail.polis;

import org.junit.jupiter.api.Assertions;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

public class PersistentTest extends BaseTest {

    @DaoTest(stage = 2)
    void persistent(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("k1", "v1"));
        dao.close();

        dao = DaoFactory.Factory.reopen(dao);
        assertSame(dao.get("k1"), entry("k1", "v1"));
    }

    @DaoTest(stage = 2)
    void cleanup(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("k1", "v1"));
        dao.close();

        Config config = DaoFactory.Factory.extractConfig(dao);
        Files.walkFileTree(config.basePath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
        dao = DaoFactory.Factory.reopen(dao);

        Assertions.assertNull(dao.get("k1"));
    }

    @DaoTest(stage = 2)
    void persistentPreventInMemoryStorage(Dao<String, Entry<String>> dao) throws IOException {
        int keys = 200_000;

        // Fill
        List<Entry<String>> entries = entries("k", "v", keys);
        entries.forEach(dao::upsert);
        dao.close();

        // Materialize to consume heap
        List<Entry<String>> tmp = new ArrayList<>(entries);

        Entry<String> entry = DaoFactory.Factory.reopen(dao).get(keyAt("k", keys / 2));
        assertSame(
                entry,
                entries.get(keys / 2)
        );

        assertSame(
                tmp.get(keys - 1),
                entries.get(keys - 1)
        );
    }

    @DaoTest(stage = 2)
    void replaceWithClose(Dao<String, Entry<String>> dao) throws IOException {
        final String key = "key";
        Entry<String> e1 = entry(key, "value1");
        Entry<String> e2 = entry(key, "value2");

        // Initial insert
        try (Dao<String, Entry<String>> dao1 = dao) {
            dao1.upsert(e1);

            assertSame(dao1.get(key), e1);
        }

        // Reopen and replace
        try (Dao<String, Entry<String>> dao2 = DaoFactory.Factory.reopen(dao)) {
            assertSame(dao2.get(key), e1);
            dao2.upsert(e2);
            assertSame(dao2.get(key), e2);
        }

        // Reopen and check
        try (Dao<String, Entry<String>> dao3 = DaoFactory.Factory.reopen(dao)) {
            assertSame(dao3.get(key), e2);
        }
    }

}
