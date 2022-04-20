package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.aliases.SSTable;
import ru.mail.polis.pavelkovalenko.dto.FileMeta;
import ru.mail.polis.pavelkovalenko.iterators.MergeIterator;
import ru.mail.polis.pavelkovalenko.utils.DaoUtils;
import ru.mail.polis.pavelkovalenko.utils.Runnables;
import ru.mail.polis.pavelkovalenko.visitors.ConfigVisitor;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMDao implements Dao<ByteBuffer, Entry<ByteBuffer>> {

    private static final int N_MEMORY_SSTABLES = 2;
    private final BlockingQueue<SSTable> sstablesForWrite = new LinkedBlockingQueue<>(N_MEMORY_SSTABLES);
    private final BlockingDeque<SSTable> sstablesForFlush = new LinkedBlockingDeque<>(N_MEMORY_SSTABLES);
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    private final Lock interThreadedLock = new ReentrantLock();
    private final ExecutorService service = Executors.newCachedThreadPool();
    private final Runnables runnables;

    private final Config config;
    private final Serializer serializer;
    private final AtomicInteger sstablesSize = new AtomicInteger(0);
    private final AtomicBoolean filesAppearedSinceLastCompact = new AtomicBoolean();

    private long curBytesForEntries;

    public LSMDao(Config config) throws IOException {
        this.config = config;
        serializer = new Serializer(config, sstablesSize);
        Files.walkFileTree(config.basePath(), new ConfigVisitor(config, sstablesSize, serializer));

        for (int i = 0; i < N_MEMORY_SSTABLES; ++i) {
            sstablesForWrite.add(new SSTable());
        }

        runnables = new Runnables(config, sstablesSize, filesAppearedSinceLastCompact, serializer, interThreadedLock,
                sstablesForWrite, sstablesForFlush);
    }

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        rwlock.readLock().lock();
        interThreadedLock.lock();
        try {
            List<SSTable> memorySSTables = new LinkedList<>();
            for (SSTable sstable : sstablesForWrite) {
                if (!sstable.isEmpty()) {
                    memorySSTables.add(sstable);
                }
            }
            // The newest SSTables to be flushed is the most priority for us
            Iterator<SSTable> sstablesForFlushIterator = sstablesForFlush.descendingIterator();
            while (sstablesForFlushIterator.hasNext()) {
                SSTable sstable = sstablesForFlushIterator.next();
                if (!sstable.isEmpty()) {
                    memorySSTables.add(sstable);
                }
            }
            return new MergeIterator(from, to, serializer, memorySSTables, sstablesSize);
        } catch (ReflectiveOperationException ex) {
            throw new RuntimeException(ex);
        } finally {
            interThreadedLock.unlock();
            rwlock.readLock().unlock();
        }
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        rwlock.writeLock().lock();
        interThreadedLock.lock();
        try {
            if (curBytesForEntries >= config.flushThresholdBytes()) {
                service.submit(runnables.flush);
                curBytesForEntries = 0;
            }

            if (sstablesForWrite.isEmpty()) {
                throw new RuntimeException("Very large number of upserting");
            }

            sstablesForWrite.peek().put(entry.key(), entry);
            curBytesForEntries += (entry.key().remaining() + serializer.sizeOf(entry));
        } finally {
            interThreadedLock.unlock();
            rwlock.writeLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        if (DaoUtils.nothingToFlush(sstablesForWrite)) {
            return;
        }

        rwlock.writeLock().lock();
        interThreadedLock.lock();
        try {
            if (DaoUtils.nothingToFlush(sstablesForWrite)) {
                return;
            }
            service.submit(runnables.flush);
        } finally {
            interThreadedLock.unlock();
            rwlock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        rwlock.writeLock().lock();
        try {
            if (DaoUtils.thereIsSmthToFlush(sstablesForWrite)) {
                service.submit(runnables.flush).get();
            }
            service.shutdown();

            if (!service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS)) {
                throw new RuntimeException("Very large number of tasks, impossible to close Dao");
            }

            if (!sstablesForFlush.isEmpty()) {
                throw new InterruptedIOException("Process of SSTables' flushing was interrupted");
            }

            if (sstablesForWrite.isEmpty()) {
                throw new InterruptedIOException("Memory SSTables were not backed to the queue");
            }

            if (!sstablesForWrite.peek().isEmpty()) {
                throw new InterruptedIOException("Close process can't be terminated");
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
        if (DaoUtils.noNeedsInCompact(sstablesSize, filesAppearedSinceLastCompact)) {
            return;
        }

        rwlock.writeLock().lock();
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

            service.submit(runnables.compact);
        } catch (ReflectiveOperationException ex) {
            throw new RuntimeException(ex);
        } finally {
            interThreadedLock.unlock();
            rwlock.writeLock().unlock();
        }
    }

}
