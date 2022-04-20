package ru.mail.polis.pavelkovalenko.utils;

import ru.mail.polis.Config;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.Serializer;
import ru.mail.polis.pavelkovalenko.aliases.SSTable;
import ru.mail.polis.pavelkovalenko.dto.FileMeta;
import ru.mail.polis.pavelkovalenko.iterators.MergeIterator;
import ru.mail.polis.pavelkovalenko.visitors.CompactVisitor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

public final class Runnables {

    private Config config;
    private AtomicInteger sstablesSize;
    private AtomicBoolean filesAppearedSinceLastCompact;
    private Serializer serializer;
    private Lock interThreadedLock;
    private BlockingQueue<SSTable> sstablesForWrite;
    private BlockingDeque<SSTable> sstablesForFlush;

    public Runnables(Config config, AtomicInteger sstablesSize, AtomicBoolean filesAppearedSinceLastCompact,
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

    public final Runnable flush = () -> {
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
    };

    public final Runnable compact = new Runnable() {
        @Override
        public void run() {
            if (DaoUtils.noNeedsInCompact(sstablesSize, filesAppearedSinceLastCompact)) {
                return;
            }

            Iterator<Entry<ByteBuffer>> mergeIterator;
            try {
                interThreadedLock.lock();
                try {
                    if (DaoUtils.noNeedsInCompact(sstablesSize, filesAppearedSinceLastCompact)) {
                        return;
                    }

                    if (sstablesSize.get() == 1) {
                        FileMeta meta = serializer.readMeta(serializer.get(1).dataFile());
                        if (!serializer.hasTombstones(meta)) {
                            return;
                        }
                    }

                    mergeIterator
                            = new MergeIterator(null, null, serializer, Collections.EMPTY_LIST, sstablesSize);
                    if (!mergeIterator.hasNext()) {
                        return;
                    }
                } finally {
                    interThreadedLock.unlock();
                }

                Path compactDataFile;
                Path compactIndexesFile;
                int iteration = 0;
                interThreadedLock.lock();
                try {
                    do {
                        ++iteration;
                        compactDataFile
                                = FileUtils.getFilePath(FileUtils.getCompactDataFilename(iteration), config);
                        compactIndexesFile
                                = FileUtils.getFilePath(FileUtils.getCompactIndexesFilename(iteration), config);
                    } while (Files.exists(compactDataFile));
                    Files.createFile(compactDataFile);
                    Files.createFile(compactIndexesFile);
                } finally {
                    interThreadedLock.unlock();
                }

                serializer.write(mergeIterator, compactDataFile, compactIndexesFile);
                interThreadedLock.lock();
                try {
                    Files.walkFileTree(config.basePath(),
                            new CompactVisitor(config, compactDataFile, compactIndexesFile, sstablesSize, serializer));
                    filesAppearedSinceLastCompact.set(false);
                } finally {
                    interThreadedLock.unlock();
                }
            } catch (IOException | ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }
    };
}
