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
        entries("k", "v", 200_000).forEach(dao::upsert);
        dao.close();

        List<Entry<String>> tmp = new ArrayList<>(entries("k", "v", 200_000));

        Entry<String> entry = DaoFactory.Factory.reopen(dao).get(keyAt("k", 50_023));
        assertSame(
                entry,
                entry(
                        keyAt("k", 50_023),
                        valueAt("v", 50_023)
                )
        );

        assertSame(
                tmp.get(0),
                entry(
                        keyAt("k", 0),
                        valueAt("v", 0)
                )
        );
    }

}
