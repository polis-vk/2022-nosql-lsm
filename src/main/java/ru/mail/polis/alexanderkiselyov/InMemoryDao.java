package ru.mail.polis.alexanderkiselyov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {
    private final FileOperations fileOperations;
    private final FlushManager flushManager;
    private final CompactManager compactManager;
    private final AtomicBoolean isClosed;
    private final PairsWrapper pairsWrapper;
    private NavigableMap<byte[], BaseEntry<byte[]>> pairs;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public InMemoryDao(Config config) throws IOException {
        fileOperations = new FileOperations(config);
        this.pairsWrapper = new PairsWrapper(config.flushThresholdBytes());
        flushManager = new FlushManager(pairsWrapper, fileOperations);
        compactManager = new CompactManager(fileOperations);
        isClosed = new AtomicBoolean(false);
        pairs = pairsWrapper.getPairs();
    }

    @Override
    public Iterator<BaseEntry<byte[]>> get(byte[] from, byte[] to) throws IOException {
        if (isClosed.get()) {
            return null;
        }
        lock.readLock().lock();
        try {
            Iterator<BaseEntry<byte[]>> memoryIterator;
            if (from == null && to == null) {
                memoryIterator = pairs.values().iterator();
            } else if (from == null) {
                memoryIterator = pairs.headMap(to).values().iterator();
            } else if (to == null) {
                memoryIterator = pairs.tailMap(from).values().iterator();
            } else {
                memoryIterator = pairs.subMap(from, to).values().iterator();
            }
            Iterator<BaseEntry<byte[]>> flushQueueIterator = flushManager.getFlushPairs() == null
                    ? null : flushManager.getFlushPairs().values().iterator();
            Iterator<BaseEntry<byte[]>> diskIterator = fileOperations.diskIterator(from, to);
            Iterator<BaseEntry<byte[]>> mergeIterator = MergeIterator.of(
                    List.of(
                            new IndexedPeekIterator(0, memoryIterator),
                            new IndexedPeekIterator(1, flushQueueIterator),
                            new IndexedPeekIterator(2, diskIterator)
                    ),
                    EntryKeyComparator.INSTANCE
            );
            return new SkipNullValuesIterator(new IndexedPeekIterator(0, mergeIterator));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public BaseEntry<byte[]> get(byte[] key) throws IOException {
        if (isClosed.get()) {
            return null;
        }
        lock.readLock().lock();
        try {
            Iterator<BaseEntry<byte[]>> iterator = get(key, null);
            if (!iterator.hasNext()) {
                return null;
            }
            BaseEntry<byte[]> next = iterator.next();
            if (Arrays.equals(key, next.key())) {
                return next;
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public synchronized void upsert(BaseEntry<byte[]> entry) {
        if (isClosed.get()) {
            return;
        }
        lock.readLock().lock();
        try {
            int entryValueLength = entry.value() == null ? 0 : entry.value().length;
            long delta = 2L * entry.key().length + entryValueLength;
            if (pairsWrapper.getCurrentPairSize().get() + delta >= pairsWrapper.getMaxPairSize().get()) {
                flushManager.performBackgroundFlush(pairsWrapper.getPairs(), pairsWrapper.getCurrentPairNum());
                pairsWrapper.changePairs(delta);
                pairs = pairsWrapper.getPairs();
            }
            pairsWrapper.updateSize(delta);
            pairs.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        if (pairs.size() == 0 || isClosed.get()) {
            return;
        }
        lock.writeLock().lock();
        try {
            fileOperations.flush(pairs);
            pairs.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public synchronized void compact() throws IOException {
        if (isClosed.get()) {
            return;
        }
        lock.writeLock().lock();
        try {
            compactManager.performCompact(pairs.size() != 0);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (isClosed.get()) {
            return;
        }
        lock.writeLock().lock();
        try {
            flush();
            flushManager.closeFlushThreadService();
            compactManager.closeCompactThreadService();
            fileOperations.clearFileIterators();
            pairs.clear();
            isClosed.set(true);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
