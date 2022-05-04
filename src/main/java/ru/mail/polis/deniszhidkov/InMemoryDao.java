package ru.mail.polis.deniszhidkov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


import static ru.mail.polis.deniszhidkov.DaoUtils.DATA_FILE_NAME;
import static ru.mail.polis.deniszhidkov.DaoUtils.OFFSETS_FILE_NAME;

public class InMemoryDao implements Dao<String, BaseEntry<String>> {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryDao.class);
    private static final String DAO_CLOSED_EXCEPTION_TEXT = "DAO has been closed"; // Требование CodeClimate
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final DaoUtils utils;
    private final ExecutorService executor;
    private final Queue<Runnable> compactTasks = new ConcurrentLinkedQueue<>();
    private final Queue<Runnable> flushTasks = new ConcurrentLinkedQueue<>();
    private final ReadWriteLock flushLock = new ReentrantReadWriteLock();
    private final ReadWriteLock upsertLock = new ReentrantReadWriteLock();
    private final long flushThresholdBytes;
    private volatile State state;
    private Future<?> compactTask;

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
        // Если закроются readers заново вызовем добавление итераторов, дождавшись завершения compact
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
            try {
                compactTask.get();
            } catch (ExecutionException ex) {
                throw new IllegalStateException("Compaction has failed", ex);
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
                return null;
            }
            currentState = this.state;
            utils.addAllIterators(iteratorsQueue,
                    currentState.inMemory,
                    currentState.inFlushing,
                    currentState.readers,
                    from,
                    to);
        }
        return iteratorsQueue.isEmpty() ? Collections.emptyIterator() : new MergeIterator(iteratorsQueue);
    }

    @Override
    public BaseEntry<String> get(String key) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException(DAO_CLOSED_EXCEPTION_TEXT);
        }
        State currentState = this.state;
        BaseEntry<String> value = utils.findInMemoryByKey(key, state.inMemory, state.inFlushing);
        if (value != null) {
            return value.value() == null ? null : value;
        }
        // Если закроются readers снова вызовем поиск по всем readers, дождавшись завершения compact
        try {
            value = utils.findInStorageByKey(key, currentState.readers);
        } catch (IllegalStateException e) {
            try {
                compactTask.get();
            } catch (ExecutionException ex) {
                throw new IllegalStateException("Compaction has failed");
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
                return null;
            }
            currentState = this.state;
            value = utils.findInStorageByKey(key, currentState.readers);
        }
        if (value == null) {
            return null;
        }
        return value.value() == null ? null : value;
    }

    @Override
    public void upsert(BaseEntry<String> entry) {
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
            prepareAndStartFlush(currentState);
        }
    }

    @Override
    public synchronized void flush() {
        if (isClosed.get()) {
            throw new IllegalStateException(DAO_CLOSED_EXCEPTION_TEXT);
        }
        State currentState = this.state;

        if (currentState.inMemory.isEmpty()) {
            return;
        }

        prepareAndStartFlush(currentState);
    }

    private void prepareAndStartFlush(State currentState) {
        if (!flushTasks.isEmpty()) {
            throw new IllegalStateException("Flush queue overflow");
        }
        flushTasks.offer(this::backgroundFlush);

        upsertLock.writeLock().lock();
        try {
            this.state = currentState.beforeFlush();
        } finally {
            upsertLock.writeLock().unlock();
        }

        if (flushTasks.peek() != null) {
            executor.submit(flushTasks.peek());
        }
    }

    private void backgroundFlush() {
        State currentState = this.state;
        try {
            // Если есть compact в процессе дождёмся сначала, когда всё скомпактиться в один файл
            flushLock.writeLock().lock();
            try {
                currentState.writer.writeDAO(currentState.inFlushing);
                currentState.readers.add(0,
                        new DaoReader(utils.resolvePath(DATA_FILE_NAME, currentState.getSizeOfStorage()),
                                utils.resolvePath(OFFSETS_FILE_NAME, currentState.getSizeOfStorage()))
                );
            } finally {
                flushLock.writeLock().unlock();
            }
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
            try {
                utils.closeReaders(currentState.readers);
            } catch (IOException ex) {
                LOG.error("Can't close storage: ", ex);
            }
        }
    }

    @Override
    public synchronized void compact() throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException(DAO_CLOSED_EXCEPTION_TEXT);
        }
        if (!compactTasks.isEmpty()) {
            return;
        }
        compactTasks.offer(this::backgroundCompact);

        if (compactTasks.peek() != null) {
            compactTask = executor.submit(compactTasks.peek());
        }
    }

    private void backgroundCompact() {
        State currentState = this.state;
        if (currentState.getSizeOfStorage() <= 1) {
            return;
        }
        try {
            Compacter compacter = new Compacter(utils);
            CopyOnWriteArrayList<DaoReader> readers;
            DaoWriter writer;
            flushLock.writeLock().lock();
            // Если есть flush в процессе дождёмся сначала, когда запишется новый файл
            try {
                compacter.writeData(currentState.readers);
                compacter.finishCompact();
                readers = utils.initDaoReaders();
                writer = new DaoWriter(utils.resolvePath(DATA_FILE_NAME, readers.size()),
                        utils.resolvePath(OFFSETS_FILE_NAME, readers.size())
                );
            } finally {
                flushLock.writeLock().unlock();
            }

            upsertLock.writeLock().lock();
            try {
                this.state = currentState.afterCompact(readers, writer);
                compactTasks.clear();
            } finally {
                upsertLock.writeLock().unlock();
            }

        } catch (Exception e) {
            LOG.error("Compact has failed: ", e);
            try {
                utils.closeReaders(currentState.readers);
            } catch (IOException ex) {
                LOG.error("Can't close storage: ", ex);
            }
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

        private final AtomicLong storageMemoryUsage;
        private final ConcurrentNavigableMap<String, BaseEntry<String>> inMemory;
        private final ConcurrentNavigableMap<String, BaseEntry<String>> inFlushing;
        private final CopyOnWriteArrayList<DaoReader> readers;
        private final DaoWriter writer;

        State(ConcurrentNavigableMap<String, BaseEntry<String>> inMemory,
              ConcurrentNavigableMap<String, BaseEntry<String>> inFlushing,
              CopyOnWriteArrayList<DaoReader> readers,
              DaoWriter writer,
              long memoryUsage
        ) {
            this.storageMemoryUsage = new AtomicLong(memoryUsage);
            this.inMemory = inMemory;
            this.inFlushing = inFlushing;
            this.readers = readers;
            this.writer = writer;
        }

        static State newState(CopyOnWriteArrayList<DaoReader> readers, DaoWriter writer) {
            return new State(new ConcurrentSkipListMap<>(), new ConcurrentSkipListMap<>(), readers, writer, 0);
        }

        State beforeFlush() {
            return new State(new ConcurrentSkipListMap<>(), inMemory, readers, writer, 0);
        }

        State afterFlush(DaoWriter writer) {
            return new State(inMemory, new ConcurrentSkipListMap<>(), readers, writer, this.storageMemoryUsage.get());
        }

        State afterCompact(CopyOnWriteArrayList<DaoReader> readers, DaoWriter writer) {
            return new State(inMemory, inFlushing, readers, writer, this.storageMemoryUsage.get());
        }

        int getSizeOfStorage() {
            return readers.size();
        }
    }
}
