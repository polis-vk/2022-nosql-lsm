package ru.mail.polis.pavelkovalenko.utils;

import ru.mail.polis.Config;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.Serializer;
import ru.mail.polis.pavelkovalenko.aliases.SSTable;
import ru.mail.polis.pavelkovalenko.iterators.MergeIterator;
import ru.mail.polis.pavelkovalenko.visitors.CompactVisitor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

public final class Runnables {

    private final Config config;
    private final AtomicInteger sstablesSize;
    private final Serializer serializer;
    private final ReadWriteLock rwlock;
    private final BlockingQueue<SSTable> sstablesForWrite;
    private final BlockingQueue<SSTable> sstablesForFlush;

    public Runnables(Config config, AtomicInteger sstablesSize, Serializer serializer, ReadWriteLock rwlock,
                     BlockingQueue<SSTable> sstablesForWrite, BlockingQueue<SSTable> sstablesForFlush) {
        this.config = config;
        this.sstablesSize = sstablesSize;
        this.serializer = serializer;
        this.rwlock = rwlock;
        this.sstablesForWrite = sstablesForWrite;
        this.sstablesForFlush = sstablesForFlush;
    }

    public final Runnable FLUSH = new Runnable() {
        @Override
        public void run() {
            Path dataFile;
            Path indexesFile;
            try {
                synchronized (sstablesSize) {
                    int fileOrdinal = sstablesSize.get();
                    do {
                        ++fileOrdinal;
                        String priorityStr = String.valueOf(fileOrdinal);
                        dataFile = Utils.getFilePath(Utils.getDataFilename(priorityStr), config);
                        indexesFile = Utils.getFilePath(Utils.getIndexesFilename(priorityStr), config);
                    } while (Files.exists(dataFile));
                    Files.createFile(dataFile);
                    Files.createFile(indexesFile);
                }

                SSTable memorySSTable;
                rwlock.writeLock().lock();
                try {
                    memorySSTable = sstablesForWrite.remove();
                    sstablesForFlush.add(memorySSTable);
                } finally {
                    rwlock.writeLock().unlock();
                }

                serializer.write(memorySSTable.values().iterator(), dataFile, indexesFile);

                rwlock.writeLock().lock();
                try {
                    sstablesSize.incrementAndGet();
                    sstablesForFlush.remove(memorySSTable);
                    memorySSTable.clear();
                    sstablesForWrite.add(memorySSTable);
                } finally {
                    rwlock.writeLock().unlock();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };

    public final Runnable COMPACT = new Runnable() {
        @Override
        public void run() {
            try {
                Iterator<Entry<ByteBuffer>> mergeIterator
                        = new MergeIterator(null, null, serializer, Utils.EMPTY_SSTABLE, sstablesSize.get());
                if (!mergeIterator.hasNext()) {
                    return;
                }

                Path compactDataFile = Utils.getFilePath(Utils.COMPACT_DATA_FILENAME, config);
                Path compactIndexesFile = Utils.getFilePath(Utils.COMPACT_INDEXES_FILENAME, config);
                serializer.write(mergeIterator, compactDataFile, compactIndexesFile);
                Files.walkFileTree(config.basePath(),
                        new CompactVisitor(config, compactDataFile, compactIndexesFile, sstablesSize));
            } catch (IOException | ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }
    };
}
