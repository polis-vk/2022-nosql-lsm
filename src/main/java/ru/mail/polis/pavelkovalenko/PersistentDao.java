package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.iterators.MergeIterator;
import ru.mail.polis.pavelkovalenko.utils.Utils;
import ru.mail.polis.pavelkovalenko.visitors.CompactVisitor;
import ru.mail.polis.pavelkovalenko.visitors.ConfigVisitor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentDao implements Dao<ByteBuffer, Entry<ByteBuffer>> {

    private final ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> memorySSTable = new ConcurrentSkipListMap<>();
    private final AtomicInteger sstablesSize = new AtomicInteger(0);
    private final Config config;
    private final Serializer serializer;
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();

    public PersistentDao(Config config) throws IOException {
        this.config = config;
        this.serializer = new Serializer(sstablesSize, config);
        Files.walkFileTree(config.basePath(), new ConfigVisitor(config, sstablesSize, serializer));
    }

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        rwlock.readLock().lock();
        try {
            return new MergeIterator(from, to, serializer, memorySSTable, sstablesSize.get());
        } catch (ReflectiveOperationException ex) {
            throw new RuntimeException(ex);
        } finally {
            rwlock.readLock().unlock();
        }
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        memorySSTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        rwlock.writeLock().lock();
        try {
            if (memorySSTable.isEmpty()) {
                return;
            }

            sstablesSize.incrementAndGet();
            String priorityStr = sstablesSize.toString();
            Path dataFile = Utils.getFilePath(Utils.getDataFilename(priorityStr), config);
            Path indexesFile = Utils.getFilePath(Utils.getIndexesFilename(priorityStr), config);
            serializer.write(memorySSTable.values().iterator(), dataFile, indexesFile);
            memorySSTable.clear();
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        rwlock.writeLock().lock();
        try {
            if (memorySSTable.isEmpty()) {
                return;
            }
            flush();
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        rwlock.writeLock().lock();
        try {
            if (memorySSTable.isEmpty() && sstablesSize.get() == 0) {
                return;
            }

            Iterator<Entry<ByteBuffer>> mergeIterator = get(null, null);
            if (!mergeIterator.hasNext()) {
                return;
            }

            Path compactDataFile = Utils.getFilePath(Utils.COMPACT_DATA_FILENAME, config);
            Path compactIndexesFile = Utils.getFilePath(Utils.COMPACT_INDEXES_FILENAME, config);
            serializer.write(mergeIterator, compactDataFile, compactIndexesFile);
            Files.walkFileTree(config.basePath(), new CompactVisitor(config, compactDataFile, compactIndexesFile, sstablesSize));
            memorySSTable.clear();
        } finally {
            rwlock.writeLock().unlock();
        }
    }

}
