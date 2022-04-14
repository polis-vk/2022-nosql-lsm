package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.iterators.MergeIterator;
import ru.mail.polis.pavelkovalenko.utils.Utils;
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
        Files.walkFileTree(config.basePath(), new ConfigVisitor(sstablesSize));
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
        rwlock.writeLock().lock();
        try {
            memorySSTable.put(entry.key(), entry);
        } finally {
            rwlock.writeLock().unlock();
        }
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

}
