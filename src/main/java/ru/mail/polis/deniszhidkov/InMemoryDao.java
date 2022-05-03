package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ru.mail.polis.deniszhidkov.DaoUtils.DATA_FILE_NAME;
import static ru.mail.polis.deniszhidkov.DaoUtils.OFFSETS_FILE_NAME;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private static final String DAO_CLOSED_EXCEPTION_TEXT = "DAO has been closed"; // Требование CodeClimate
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final DaoUtils utils;
    private final ExecutorService executor;
    private final ReadWriteLock upsertLock = new ReentrantReadWriteLock();
    private final long flushThresholdBytes;
    private volatile State state;
    private final Queue<Runnable> flushTasks = new ConcurrentLinkedQueue<>();
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryDao.class);

    public InMemoryDao(Config config) throws IOException {
        this.flushThresholdBytes = config.flushThresholdBytes();
        this.utils = new DaoUtils(config.basePath());
        Compacter compacter = new Compacter(utils);
        compacter.finishCompact();
        utils.validateDAOFiles();
        CopyOnWriteArrayList<DaoReader> readers = utils.initDaoReaders();
        DaoWriter writer = new DaoWriter(utils.resolvePath(DATA_FILE_NAME, readers.size()),
                utils.resolvePath(OFFSETS_FILE_NAME, readers.size())
        );
        this.state = State.newState(readers, writer);
        this.executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "DaoBackgroundThread"));
    }

    @Override
    public Iterator<BaseEntry<String>> get(String from, String to) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException(DAO_CLOSED_EXCEPTION_TEXT);
        }
        State currentState = this.state;
        Queue<PriorityPeekIterator> iteratorsQueue = new PriorityQueue<>(
                Comparator.comparing((PriorityPeekIterator o) ->
                        o.peek().key()).thenComparingInt(PriorityPeekIterator::getPriorityIndex)
        );
        // Если закроются readers заново вызовем get
        try {
            utils.addAllIterators(iteratorsQueue,
                    currentState.inMemory,
                    currentState.inFlushing,
                    currentState.readers,
                    from,
                    to
            );
        } catch (IllegalStateException e) {
            iteratorsQueue.clear();
            utils.addAllIterators(iteratorsQueue,
                    currentState.inMemory,
                    currentState.inFlushing,
                    currentState.readers,
                    from,
                    to); // FIXME it won't work
        }
        return iteratorsQueue.isEmpty() ? Collections.emptyIterator() : new MergeIterator(iteratorsQueue);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException(DAO_CLOSED_EXCEPTION_TEXT);
        }
        State currentState = this.state;
        BaseEntry<String> value = currentState.inMemory.get(key);
        if (value == null) {
            value = currentState.inFlushing.get(key);
            if (value != null) {
                return value;
            }
            // Если закроются readers снова вызовем get
            try {
                for (int i = 0; i < currentState.getSizeOfStorage(); i++) {
                    value = currentState.readers.get(i).findByKey(key);
                    if (value != null) {
                        return value.value() == null ? null : value;
                    }
                }
            } catch (IllegalStateException e) {
                get(key); // FIXME recursion
            }
            value = new BaseEntry<>(null, null);
        }
        return value.value() == null ? null : value;
    }

    @Override
    public void upsert(BaseEntry<String> entry) throws UncheckedIOException {
        if (isClosed.get()) {
            throw new IllegalStateException(DAO_CLOSED_EXCEPTION_TEXT);
        }
        State currentState = this.state;
        long entrySize = DaoUtils.getEntrySize(entry);
        long currentStorageSize;
        upsertLock.readLock().lock();
        try {
            currentState.inMemory.put(entry.key(), entry);
            currentStorageSize = currentState.storageMemoryUsage.addAndGet(entrySize);
        } finally {
            upsertLock.readLock().unlock();
        }
        if (currentStorageSize >= flushThresholdBytes) {
            if (!flushTasks.isEmpty()) {
                throw new IllegalStateException("Flush queue overflow");
            }
            flushTasks.offer(() -> {
                try {
                    backgroundFlush();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            upsertLock.writeLock().lock();
            try {
                this.state = currentState.beforeFlush();
            } finally {
                upsertLock.writeLock().unlock();
            }

            executor.submit(flushTasks.peek());
        }
    }

    @Override
    public synchronized void flush() throws IOException, UncheckedIOException {
        if (isClosed.get()) {
            throw new IllegalStateException(DAO_CLOSED_EXCEPTION_TEXT);
        }
        if (!flushTasks.isEmpty()) { // FIXME race
            throw new IllegalStateException("Flush queue overflow");
        }
        flushTasks.offer(() -> {
            try {
                backgroundFlush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        State currentState = this.state;

        upsertLock.writeLock().lock();
        try {
            this.state = currentState.beforeFlush();
        } finally {
            upsertLock.writeLock().unlock();
        }

        executor.submit(flushTasks.peek());
    }

    private void backgroundFlush() throws IOException {
        State currentState = this.state;

        try {
            currentState.writer.writeDAO(currentState.inFlushing);

            currentState.readers.add(0,
                    new DaoReader(utils.resolvePath(DATA_FILE_NAME, currentState.getSizeOfStorage()),
                            utils.resolvePath(OFFSETS_FILE_NAME, currentState.getSizeOfStorage()))
            );
            DaoWriter writer = new DaoWriter(utils.resolvePath(DATA_FILE_NAME, currentState.getSizeOfStorage()),
                    utils.resolvePath(OFFSETS_FILE_NAME, currentState.getSizeOfStorage())
            );

            upsertLock.writeLock().lock();
            try {
                this.state = currentState.afterFlush(writer);
                flushTasks.clear();
            } finally {
                upsertLock.writeLock().unlock();
            }
        } catch (Exception e) {
            LOG.error("Flush has failed: ", e);
            utils.closeReaders(currentState.readers);
        }
    }

    @Override
    public void compact() throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException(DAO_CLOSED_EXCEPTION_TEXT);
        }
        executor.submit(() -> { // FIXME unnecessary compact tasks
            try {
                backgroundCompact();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private void backgroundCompact() throws IOException {
        State currentState = this.state;
        if (currentState.getSizeOfStorage() <= 1) {
            return;
        }
        Compacter compacter = new Compacter(utils);
        compacter.writeData(currentState.readers);
        compacter.finishCompact();
        CopyOnWriteArrayList<DaoReader> readers = utils.initDaoReaders();
        DaoWriter writer = new DaoWriter(utils.resolvePath(DATA_FILE_NAME, readers.size()),
                utils.resolvePath(OFFSETS_FILE_NAME, readers.size())
        );
        upsertLock.writeLock().lock();
        try {
            this.state = currentState.afterCompact(readers, writer);
        } finally {
            upsertLock.writeLock().unlock();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (isClosed.getAndSet(true)) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Close awaits too long");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        State currentState = this.state;
        utils.closeReaders(currentState.readers);
        currentState.readers.clear();
        upsertLock.writeLock().lock();
        try {
            if (!currentState.inMemory.isEmpty()) {
                currentState.writer.writeDAO(currentState.inMemory);
                currentState.inMemory.clear();
            }
        } finally {
            upsertLock.writeLock().unlock();
        }
    }

    private static class State {

        private final AtomicLong storageMemoryUsage = new AtomicLong(0);
        private final ConcurrentNavigableMap<String, BaseEntry<String>> inMemory;
        private final ConcurrentNavigableMap<String, BaseEntry<String>> inFlushing;
        private final CopyOnWriteArrayList<DaoReader> readers;
        private final DaoWriter writer;

        State(ConcurrentNavigableMap<String, BaseEntry<String>> inMemory,
              ConcurrentNavigableMap<String, BaseEntry<String>> inFlushing,
              CopyOnWriteArrayList<DaoReader> readers,
              DaoWriter writer
        ) {
            this.inMemory = inMemory;
            this.inFlushing = inFlushing;
            this.readers = readers;
            this.writer = writer;
        }

        static State newState(CopyOnWriteArrayList<DaoReader> readers, DaoWriter writer) {
            return new State(new ConcurrentSkipListMap<>(), new ConcurrentSkipListMap<>(), readers, writer);
        }

        State beforeFlush() {
            storageMemoryUsage.set(0);
            return new State(new ConcurrentSkipListMap<>(), inMemory, readers, writer);
        }

        State afterFlush(DaoWriter writer) {
            return new State(inMemory, new ConcurrentSkipListMap<>(), readers, writer);
        }

        State afterCompact(CopyOnWriteArrayList<DaoReader> readers, DaoWriter writer) {
            return new State(inMemory, inFlushing, readers, writer);
        }

        int getSizeOfStorage() {
            return readers.size();
        }
    }
}
