package ru.mail.polis.pavelkovalenko.runnables;

import ru.mail.polis.Config;
import ru.mail.polis.pavelkovalenko.Serializer;
import ru.mail.polis.pavelkovalenko.aliases.SSTable;
import ru.mail.polis.pavelkovalenko.utils.DaoUtils;
import ru.mail.polis.pavelkovalenko.utils.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ConcurrentModificationException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

public class FlushRunnable implements Runnable {

    private final Config config;
    private final AtomicInteger sstablesSize;
    private final AtomicBoolean filesAppearedSinceLastCompact;
    private final Serializer serializer;
    private final Lock interThreadedLock;
    private final BlockingQueue<SSTable> sstablesForWrite;
    private final BlockingDeque<SSTable> sstablesForFlush;

    public FlushRunnable(Config config, AtomicInteger sstablesSize, AtomicBoolean filesAppearedSinceLastCompact,
                 Serializer serializer, Lock interThreadedLock,
                 BlockingQueue<SSTable> sstablesForWrite, BlockingDeque<SSTable> sstablesForFlush) {
        this.config = config;
        this.sstablesSize = sstablesSize;
        this.filesAppearedSinceLastCompact = filesAppearedSinceLastCompact;
        this.serializer = serializer;
        this.interThreadedLock = interThreadedLock;
        this.sstablesForWrite = sstablesForWrite;
        this.sstablesForFlush = sstablesForFlush;
    }

    @Override
    public void run() {
        if (DaoUtils.nothingToFlush(sstablesForWrite)) {
            return;
        }

        Path dataFile;
        Path indexesFile;
        try {
            interThreadedLock.lock();
            try {
                if (DaoUtils.nothingToFlush(sstablesForWrite)) {
                    return;
                }

                int fileOrdinal = sstablesSize.get();
                do {
                    ++fileOrdinal;
                    dataFile = FileUtils.getFilePath(FileUtils.getDataFilename(fileOrdinal), config);
                    indexesFile = FileUtils.getFilePath(FileUtils.getIndexesFilename(fileOrdinal), config);
                } while (Files.exists(dataFile));
                Files.createFile(dataFile);
                Files.createFile(indexesFile);
            } finally {
                interThreadedLock.unlock();
            }

            SSTable memorySSTable;
            interThreadedLock.lock();
            try {
                if (DaoUtils.nothingToFlush(sstablesForWrite)) {
                    Files.delete(dataFile);
                    Files.delete(indexesFile);
                    return;
                }

                memorySSTable = sstablesForWrite.remove();
                sstablesForFlush.add(memorySSTable);
            } finally {
                interThreadedLock.unlock();
            }

            serializer.write(memorySSTable.values().iterator(), dataFile, indexesFile);

            interThreadedLock.lock();
            try {
                sstablesSize.incrementAndGet();
                filesAppearedSinceLastCompact.set(true);
                if (!sstablesForFlush.remove(memorySSTable)) {
                    throw new ConcurrentModificationException("Unexpected SSTable's removing");
                }
                memorySSTable.clear();
                sstablesForWrite.add(memorySSTable);
            } finally {
                interThreadedLock.unlock();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
