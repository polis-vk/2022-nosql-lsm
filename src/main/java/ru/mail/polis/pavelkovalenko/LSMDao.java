package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.aliases.SSTable;
import ru.mail.polis.pavelkovalenko.iterators.MergeIterator;
import ru.mail.polis.pavelkovalenko.utils.Runnables;
import ru.mail.polis.pavelkovalenko.utils.Utils;
import ru.mail.polis.pavelkovalenko.visitors.ConfigVisitor;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMDao implements Dao<ByteBuffer, Entry<ByteBuffer>> {

    private final int N_MEMORY_SSTABLES = 2;
    private final short TIMEOUT = Short.MAX_VALUE >>> 7;
    private final BlockingQueue<SSTable> sstablesForWrite = new LinkedBlockingQueue<>(N_MEMORY_SSTABLES);
    private final BlockingQueue<SSTable> sstablesForFlush = new LinkedBlockingQueue<>(N_MEMORY_SSTABLES);
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    private final ExecutorService service = Executors.newCachedThreadPool();
    private final Runnables runnables;

    private AtomicInteger sstablesSize = new AtomicInteger(0);
    private final Config config;
    private final Serializer serializer;

    private long curBytesForEntries;

    public LSMDao(Config config) throws IOException {
        this.config = config;
        serializer = new Serializer(config, sstablesSize);
        Files.walkFileTree(config.basePath(), new ConfigVisitor(config, sstablesSize, serializer));

        for (int i = 0; i < N_MEMORY_SSTABLES; ++i) {
            sstablesForWrite.add(new SSTable());
        }

        runnables = new Runnables(config, sstablesSize, serializer, rwlock, sstablesForWrite, sstablesForFlush);
    }

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        rwlock.readLock().lock();
        try {
            return new MergeIterator(from, to, serializer, memorySSTables.peek(), sstablesSize.get());
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
            if (curBytesForEntries >= config.flushThresholdBytes()) {
                service.submit(runnables.FLUSH);
                curBytesForEntries = 0;
            }

            if (sstablesForWrite.isEmpty()) {
                throw new RuntimeException("Very large number of upserting");
            }

            sstablesForWrite.peek().put(entry.key(), entry);
            curBytesForEntries += (entry.key().remaining() + serializer.sizeOf(entry));
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        rwlock.writeLock().lock();
        try {
            if (sstablesForWrite.isEmpty()) {
                return;
            }
            service.submit(runnables.FLUSH);
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        rwlock.writeLock().lock();
        try {
            service.shutdown();
            if (!service.awaitTermination(TIMEOUT, TimeUnit.SECONDS)) {
                throw new RuntimeException("Very large number of tasks, impossible to close Dao");
            }

            if (sstablesForFlush.isEmpty()) {
                throw new InterruptedIOException("Memory SSTables were not backed to the queue");
            }

            if (!sstablesForWrite.peek().isEmpty()) {
                service.submit(runnables.FLUSH).get();
            }

            sstablesForWrite.clear();
            while (sstablesSize.get() > 0) {
                sstablesSize.decrementAndGet();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        if (sstablesSize.get() <= 1) {
            return;
        }
        rwlock.writeLock().lock();
        try {
            service.submit(runnables.COMPACT);
        } finally {
            rwlock.writeLock().unlock();
        }
    }

}
