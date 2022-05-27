package ru.mail.polis.nikitazadorotskas;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final Utils utils;
    private final ExecutorService executor
            = Executors.newSingleThreadExecutor(r -> new Thread(r, "Background DAO thread"));
    private final Config config;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private State state;
    private Future<?> flushTask;
    private Future<?> compactTask;

    public PersistentDao(Config config) throws IOException {
        this.config = config;
        utils = new Utils(config);
        state = new State(new Memory(), null, new Storage(config));
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) throws IOException {
        return new MergedIterator(this::getIterators, from, to, utils);
    }

    private List<PeekIterator> getIterators(MemorySegment from, MemorySegment to) {
        State currentState = state;
        List<PeekIterator> iterators = currentState.storage.getFilesIterators(from, to);
        int numberOfMemoryIterators = currentState.storage.size();

        if (currentState.flushingMemory != null) {
            iterators.add(currentState.flushingMemory.getPeekIterator(numberOfMemoryIterators++, from, to));
        }

        iterators.add(currentState.memory.getPeekIterator(numberOfMemoryIterators, from, to));
        return iterators;
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) throws IOException {
        State currentState = state;
        BaseEntry<MemorySegment> result;

        result = currentState.memory.get(key);

        if (result == null && currentState.flushingMemory != null) {
            result = currentState.flushingMemory.get(key);
        }

        if (result != null) {
            return utils.checkIfWasDeleted(result);
        }

        if (currentState.storage.size() == 0) {
            return null;
        }

        return currentState.storage.getFromStorage(key);
    }

    @Override
    public void compact() throws IOException {
        if (state.storage.isNotAlive()) {
            throw new IllegalStateException("Called compact after close");
        }

        compactTask = executor.submit(() -> state.storage.doCompact());
    }

    @Override
    public synchronized void flush() throws IOException {
        if (state.storage.isNotAlive()) {
            throw new IllegalStateException("Called flush after close");
        }

        if (state.memory.isEmpty()) {
            return;
        }

        if (flushTask != null && !flushTask.isDone()) {
            throw new IOException("Too many flushes: one table is being written and one is full");
        }

        lock.writeLock().lock();
        try {
            state = state.prepareForFlush();
        } finally {
            lock.writeLock().unlock();
        }

        flushTask = executor.submit(this::doFlush);
    }

    private void doFlush() {
        state.storage.doFlush(state.flushingMemory);
        state = state.finishFlush();
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        State currentState = state;

        lock.readLock().lock();
        try {
            currentState.memory.put(entry);
        } finally {
            lock.readLock().unlock();
        }

        try {
            if (currentState.memory.getBytesSize() >= config.flushThresholdBytes()) {
                flush();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        State currentState = state;
        if (currentState.storage.isNotAlive()) {
            return;
        }

        try {
            waitTask(compactTask);
            waitTask(flushTask);
            flush();
            waitTask(flushTask);
        } catch (ExecutionException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
        currentState.storage.close();
        executor.shutdown();

        state = null;
    }

    private void waitTask(Future<?> task) throws ExecutionException, InterruptedException {
        if (task != null) {
            task.get();
        }
    }
}
