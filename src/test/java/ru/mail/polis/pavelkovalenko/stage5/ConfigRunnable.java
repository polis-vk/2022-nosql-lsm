package ru.mail.polis.pavelkovalenko.stage5;

import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.test.DaoFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;

public class ConfigRunnable implements Runnable {

    private final Config config;
    private final AtomicInteger filesCount;

    public ConfigRunnable(Dao<String, Entry<String>> dao, AtomicInteger filesCount) {
        this.config = DaoFactory.Factory.extractConfig(dao);
        this.filesCount = filesCount;
    }

    @Override
    public void run() {
        try {
            int curFilesCount;
            while (filesCount.get()
                    != (curFilesCount = (int) Files.list(config.basePath()).count())) {
                filesCount.set(curFilesCount);
                Thread.sleep(500);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
