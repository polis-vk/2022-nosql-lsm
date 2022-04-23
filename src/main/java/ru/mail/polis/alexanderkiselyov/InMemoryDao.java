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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {
    private final FileOperations fileOperations;
    private final AtomicBoolean isClosed;
    private final NavigableMap<byte[], BaseEntry<byte[]>> pairs0;
    private final NavigableMap<byte[], BaseEntry<byte[]>> pairs1;
    private final AtomicInteger pairNum;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Config config;
    private static final byte[] VERY_FIRST_KEY = new byte[]{};
    private State state;

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        fileOperations = new FileOperations(config);
        isClosed = new AtomicBoolean(false);
        pairs0 = new ConcurrentSkipListMap<>(Arrays::compare);
        pairs1 = new ConcurrentSkipListMap<>(Arrays::compare);
        pairNum = new AtomicInteger(0);
        state = new State(getPairs(), fileOperations);
    }

    @Override
    public Iterator<BaseEntry<byte[]>> get(byte[] from, byte[] to) throws IOException {
        if (isClosed.get()) {
            return null;
        }
        State currentState = this.state;
        lock.readLock().lock();
        try {
            byte[] fromArr = from;
            if (from == null) {
                fromArr = VERY_FIRST_KEY;
            }
            ArrayList<Iterator<BaseEntry<byte[]>>> iterators = new ArrayList<>();
            if (to == null) {
                iterators.add(currentState.pairs.tailMap(fromArr).values().iterator());
            } else {
                iterators.add(currentState.pairs.subMap(fromArr, to).values().iterator());
            }
            iterators.add(currentState.getFlushingPairsIterator());
            iterators.addAll(currentState.fileOperations.diskIterators(fromArr, to));
            Iterator<BaseEntry<byte[]>> mergeIterator = MergeIterator.of(iterators, EntryKeyComparator.INSTANCE);
            return new TombstoneFilteringIterator(mergeIterator);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public BaseEntry<byte[]> get(byte[] key) throws IOException {
        if (isClosed.get()) {
            return null;
        }
        State currentState = this.state;
        lock.readLock().lock();
        try {
            BaseEntry<byte[]> result = currentState.pairs.get(key);
            if (result == null) {
                result = currentState.fileOperations.get(key);
            }
            return (result == null || result.isTombstone()) ? null : result;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(BaseEntry<byte[]> entry) {
        if (isClosed.get()) {
            return;
        }
        State currentState = this.state;
        lock.readLock().lock();
        try {
            int entryValueLength = entry.value() == null ? 0 : entry.value().length;
            long delta = 2L * entry.key().length + entryValueLength;
            if (currentState.getSize() + delta >= config.flushThresholdBytes()) {
                if (pairs0.size() > 0 && pairs1.size() > 0) {
                    throw new IllegalStateException("Unable to flush: all maps are full.");
                }
                pairNum.set(1 - pairNum.get());
                currentState.performBackgroundFlush();
                this.state = new State(getPairs(), fileOperations);
            }
            currentState.pairs.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        State currentState = this.state;
        if (currentState.pairs.size() == 0 || isClosed.get()) {
            return;
        }
        lock.writeLock().lock();
        try {
            currentState.fileOperations.flush(currentState.pairs);
            currentState.pairs.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public synchronized void compact() throws IOException {
        if (isClosed.get()) {
            return;
        }
        State currentState = this.state;
        lock.writeLock().lock();
        try {
            currentState.performCompact();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (isClosed.get()) {
            return;
        }
        State currentState = this.state;
        lock.writeLock().lock();
        try {
            currentState.closeService();
            flush();
            currentState.fileOperations.clearFileIterators();
            pairs0.clear();
            pairs1.clear();
            isClosed.set(true);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private NavigableMap<byte[], BaseEntry<byte[]>> getPairs() {
        return pairNum.get() == 0 ? pairs0 : pairs1;
    }

    private static class State {
        private final NavigableMap<byte[], BaseEntry<byte[]>> pairs;
        private NavigableMap<byte[], BaseEntry<byte[]>> flushingPairs;
        private final FileOperations fileOperations;
        private final ExecutorService service;
        private final List<Future<?>> taskResults;
        private final Logger logger;

        private State(NavigableMap<byte[], BaseEntry<byte[]>> pairs, FileOperations fileOperations) {
            this.pairs = pairs;
            this.fileOperations = fileOperations;
            service = Executors.newSingleThreadExecutor(r -> new Thread(r, "BackgroundFlushAndCompact"));
            taskResults = new ArrayList<>();
            logger = LoggerFactory.getLogger(State.class);
            flushingPairs = null;
        }

        private long getSize() {
            long size = 0;
            for (var entry : pairs.entrySet()) {
                size += 2L * entry.getKey().length;
                size += entry.getValue().isTombstone() ? 0 : entry.getValue().value().length;
            }
            return size;
        }

        private Iterator<BaseEntry<byte[]>> getFlushingPairsIterator() {
            return flushingPairs == null ? null : flushingPairs.values().iterator();
        }

        private void performBackgroundFlush() {
            flushingPairs = pairs;
            taskResults.add(service.submit(() -> {
                try {
                    fileOperations.flush(flushingPairs);
                    flushingPairs.clear();
                } catch (IOException e) {
                    logger.error("Flush operation was interrupted.", e);
                }
            }));
        }

        public synchronized void performCompact() {
            taskResults.add(service.submit(() -> {
                Iterator<BaseEntry<byte[]>> iterator;
                try {
                    iterator = new TombstoneFilteringIterator(
                            MergeIterator.of(fileOperations.diskIterators(null, null), EntryKeyComparator.INSTANCE)
                    );
                    if (!iterator.hasNext()) {
                        return;
                    }
                    fileOperations.compact(iterator, pairs.size() != 0);
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
    }
}
