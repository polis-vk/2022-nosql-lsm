package ru.mail.polis.dmitreemaximenko;

import ru.mail.polis.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileManager {
    private static final String TMP_COMPACT_FILE = "compact_log_tmp";
    private static final String TMP_FLUSH_FILE = "flush_log_tmp";
    private static final String COMPACTING_FILE = "compacting_tmp";
    private static final String LOG_NAME = "log";
    private static final int LOG_INDEX_START = 0;

    private int log_files_amount = 0;
    private final ReadWriteLock logDirectoryLock = new ReentrantReadWriteLock();
    private final Config config;

    public FileManager(Config config) {
        this.config = config;

        int logIndex = LOG_INDEX_START;
        while (true) {
            Path filename = getLogName(logIndex);
            if (Files.exists(filename)) {
                logIndex++;
            } else {
                break;
            }
        }

        log_files_amount = logIndex;
    }

    public Path getLogName(int logNumber) {
        return config.basePath().resolve(LOG_NAME + logNumber);
    }

    public Path getFirstLogFileName() {
        return config.basePath().resolve(LOG_NAME + LOG_INDEX_START);
    }

    public Path getCompactTmpFile() {
        return config.basePath().resolve(TMP_COMPACT_FILE);
    }

    public Path getFlushTmpFile() {
        return config.basePath().resolve(TMP_FLUSH_FILE);
    }

    public Path getCompactingInProcessFile() {return config.basePath().resolve(COMPACTING_FILE);}


    public Path getNextLogNameWithoutLocking() {
        return config.basePath().resolve(LOG_NAME + log_files_amount);
    }

    public Path getNextLogName() {
        logDirectoryLock.readLock().lock();
        Path result = getNextLogNameWithoutLocking();
        logDirectoryLock.readLock().unlock();
        return result;
    }

    public int addLog(Path newLogFile) throws IOException {
        logDirectoryLock.writeLock().lock();

        int newLogIndex = log_files_amount;
        try {
            Files.move(newLogFile, getNextLogNameWithoutLocking(), StandardCopyOption.ATOMIC_MOVE);
            log_files_amount++;
        } finally {
            logDirectoryLock.writeLock().unlock();
        }

        return newLogIndex;
    }

    public List<Path> getLogPaths() {
        List<Path> result = new LinkedList<>();
        logDirectoryLock.readLock().lock();
        try {
            for (int i = 0; i < log_files_amount; i++) {
                Path filename = getLogName(i);
                result.add(filename);
            }
        } finally {
            logDirectoryLock.readLock().unlock();
        }

        return result;
    }

    public void lock() {
        logDirectoryLock.writeLock().lock();
    }

    public void unlock() {
        logDirectoryLock.writeLock().unlock();
    }

    // suppose we have logs: 0 1 2 3 4 5, size equals 2
    // 1) we get 2 3 2 3 4 5
    // 2) then 2 3 4 5 4 5
    // 3) then 2 3 4 5
    // #fixMe implement this
    public void removeLogFilesWithoutLockingWithFixingFurtherLogs(int size) throws IOException {
        for (int logIndex = log_files_amount - 1; logIndex > 0; logIndex--) {
            Path filename = config.basePath().resolve(LOG_NAME + logIndex);
            Files.delete(filename);
        }
        log_files_amount = 0;
    }

    public int addLogWithoutLocking(Path newLogFile) throws IOException {
        int newLogIndex = log_files_amount;

        Files.move(newLogFile, getNextLogName(), StandardCopyOption.ATOMIC_MOVE);
        log_files_amount++;

        return newLogIndex;
    }
}
