package ru.mail.polis.alexanderkiselyov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {
    private final AtomicBoolean isClosed;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private State state;
    private final long maxThresholdBytes;
    private final ExecutorService service;
    private final List<Future<?>> taskResults;
    private final Logger logger;
    private final AtomicBoolean isBackgroundFlushing;
    private final Object object;

    public InMemoryDao(Config config) throws IOException {
        FileOperations fileOperations = new FileOperations(config);
        isClosed = new AtomicBoolean(false);
        state = State.emptyState(fileOperations);
        maxThresholdBytes = config.flushThresholdBytes();
        service = Executors.newSingleThreadExecutor(r -> new Thread(r, "BackgroundFlushAndCompact"));
        taskResults = new CopyOnWriteArrayList<>();
        logger = LoggerFactory.getLogger(InMemoryDao.class);
        isBackgroundFlushing = new AtomicBoolean(false);
        object = new Object();
    }

    @Override
    public Iterator<BaseEntry<byte[]>> get(byte[] from, byte[] to) throws IOException {
        if (isClosed.get()) {
            throw new RuntimeException("Unable to get: close operation performed.");
        }
        State currentState;
        lock.readLock().lock();
        try {
            currentState = this.state;
        } finally {
            lock.readLock().unlock();
        }
        ArrayList<Iterator<BaseEntry<byte[]>>> iterators = new ArrayList<>();
        if (from == null && to == null) {
            iterators.add(currentState.pairs.values().iterator());
        } else if (from == null) {
            iterators.add(currentState.pairs.headMap(to).values().iterator());
        } else if (to == null) {
            iterators.add(currentState.pairs.tailMap(from).values().iterator());
        } else {
            iterators.add(currentState.pairs.subMap(from, to).values().iterator());
        }
        iterators.add(currentState.getFlushingPairsIterator());
        iterators.addAll(currentState.fileOperations.diskIterators(from, to));
        Iterator<BaseEntry<byte[]>> mergeIterator = MergeIterator.of(iterators, EntryKeyComparator.INSTANCE);
        return new TombstoneFilteringIterator(mergeIterator);
    }

    @Override
    public BaseEntry<byte[]> get(byte[] key) throws IOException {
        if (isClosed.get()) {
            throw new RuntimeException("Unable to get: close operation performed.");
        }
        State currentState;
        lock.readLock().lock();
        try {
            currentState = this.state;
        } finally {
            lock.readLock().unlock();
        }
        BaseEntry<byte[]> result = currentState.pairs.get(key);
        if (result == null) {
            result = currentState.fileOperations.get(key);
        }
        return (result == null || result.isTombstone()) ? null : result;
    }

    @Override
    public void upsert(BaseEntry<byte[]> entry) {
        if (isClosed.get()) {
            throw new RuntimeException("Unable to upsert: close operation performed.");
        }
        State currentState;
        lock.writeLock().lock();
        try {
            currentState = this.state;
        } finally {
            lock.writeLock().unlock();
        }
        int entryValueLength = entry.isTombstone() ? 0 : entry.value().length;
        int delta = 2 * entry.key().length + entryValueLength;
        synchronized (object) {
            if (currentState.getSize().get() + delta >= maxThresholdBytes) {
                startFlushing();
            } else {
                currentState.updateSize(delta);
            }
        }
        currentState.pairs.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (isClosed.get()) {
            throw new RuntimeException("Unable to flush: close operation performed.");
        }
        State currentState = this.state;
        if (currentState.pairs.isEmpty()) {
            return;
        }
        startFlushing();
    }

    @Override
    public void compact() throws IOException {
        if (isClosed.get()) {
            throw new RuntimeException("Unable to compact: close operation performed.");
        }
        performCompact();
    }

    @Override
    public void close() throws IOException {
        if (isClosed.get()) {
            throw new RuntimeException("Unable to close one more time: close operation already performed.");
        }
        flush();
        isClosed.set(true);
        closeService();
        State currentState;
        lock.writeLock().lock();
        try {
            currentState = this.state;
        } finally {
            lock.writeLock().unlock();
        }
        currentState.fileOperations.clearFileIterators();
    }

    private void startFlushing() {
        if (isBackgroundFlushing.get()) {
            throw new IllegalStateException("Unable to flush: all maps are full.");
        }
        isBackgroundFlushing.set(true);
        performBackgroundFlush();
    }

    private void performBackgroundFlush() {
        State state;
        lock.writeLock().lock();
        try {
            state = this.state;
        } finally {
            lock.writeLock().unlock();
        }
        taskResults.add(service.submit(() -> {
            try {
                lock.writeLock().lock();
                try {
                    this.state = state.beforeFlushState();
                } finally {
                    lock.writeLock().unlock();
                }
                state.fileOperations.flush(state.pairs);
                lock.writeLock().lock();
                try {
                    this.state = this.state.afterFlushState();
                } finally {
                    lock.writeLock().unlock();
                }
            } catch (IOException e) {
                logger.error("Flush operation was interrupted.", e);
            } finally {
                isBackgroundFlushing.set(false);
            }
        }));
    }

    private void performCompact() {
        State state;
        lock.writeLock().lock();
        try {
            state = this.state;
        } finally {
            lock.writeLock().unlock();
        }
        taskResults.add(service.submit(() -> {
            Iterator<BaseEntry<byte[]>> iterator;
            try {
                iterator = MergeIterator.of(state.fileOperations.diskIterators(null, null),
                        EntryKeyComparator.INSTANCE);
                if (!iterator.hasNext()) {
                    return;
                }
                state.fileOperations.compact(iterator, state.pairs.size() != 0);
            } catch (IOException e) {
                logger.error("Compact operation was interrupted.", e);
            }
        }));
    }

    private void closeService() {
        service.shutdown();
        for (Future<?> taskResult : taskResults) {
            if (taskResult != null && !taskResult.isDone()) {
                try {
                    taskResult.get();
                } catch (ExecutionException | InterruptedException e) {
                    logger.error("Current thread was interrupted.", e);
                }
            }
        }
        taskResults.clear();
    }

    private static class State {
        private final NavigableMap<byte[], BaseEntry<byte[]>> pairs;
        private final NavigableMap<byte[], BaseEntry<byte[]>> flushingPairs;
        private final AtomicInteger pairsSize;
        private final FileOperations fileOperations;

        private State(NavigableMap<byte[], BaseEntry<byte[]>> pairs,
                      NavigableMap<byte[], BaseEntry<byte[]>> flushingPairs, FileOperations fileOperations) {
            this.pairs = pairs;
            this.fileOperations = fileOperations;
            this.flushingPairs = flushingPairs;
            pairsSize = new AtomicInteger(0);
        }

        private static State emptyState(FileOperations fileOperations) {
            return new State(new ConcurrentSkipListMap<>(Arrays::compare),
                    new ConcurrentSkipListMap<>(Arrays::compare), fileOperations);
        }

        private State beforeFlushState() {
            return new State(new ConcurrentSkipListMap<>(Arrays::compare), pairs, fileOperations);
        }

        private State afterFlushState() {
            return new State(pairs, new ConcurrentSkipListMap<>(Arrays::compare), fileOperations);
        }

        private AtomicInteger getSize() {
            return pairsSize;
        }

        private void updateSize(int delta) {
            pairsSize.getAndAdd(delta);
        }

        private Iterator<BaseEntry<byte[]>> getFlushingPairsIterator() {
            return flushingPairs == null ? null : flushingPairs.values().iterator();
        }
    }
}
