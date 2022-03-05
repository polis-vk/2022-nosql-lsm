package ru.mail.polis;

import org.junit.jupiter.api.Assertions;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

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

    @DaoTest
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
    }
}
