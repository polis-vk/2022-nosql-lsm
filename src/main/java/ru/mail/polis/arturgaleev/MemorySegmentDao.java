package ru.mail.polis.arturgaleev;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.BufferOverflowException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static ru.mail.polis.arturgaleev.FileDBWriter.getEntryLength;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final AtomicReference<ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>>> memory;
    // queue which is waiting for flush
    private final BlockingQueue<ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>>> flushQueue;
    // sstable which is processing now. (I'd better use flushQueue.peek() which could wait like take())
    private final BlockingQueue<ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>>> currentlyFlushing;
    // Shows the need of flush
    private final AtomicBoolean needToCompact = new AtomicBoolean();
    // Used for ordering flush and compact(compact begins only between flush cycles)
    private final Semaphore semaphore = new Semaphore(0);
    private final ExecutorService threadPool;

    private final Config config;
    private final DBReader reader;
    private final long autoFlushSize;
    private final AtomicLong currentByteSize = new AtomicLong();
    private final AtomicLong newFileId;
    private final AtomicBoolean isClosed = new AtomicBoolean();

    private final Future<?> flushFuture;
    private final Future<?> compactFuture;

    public MemorySegmentDao(Config config) throws IOException {
        this.config = config;
        autoFlushSize = config.flushThresholdBytes();
        reader = new DBReader(config.basePath());
        newFileId = new AtomicLong(reader.getBiggestFileId() + 1);

        memory = new AtomicReference<>(getNewSkipListMap());
        flushQueue = new LinkedBlockingQueue<>(1);
        currentlyFlushing = new LinkedBlockingQueue<>(1);

        threadPool = newFixedThreadPool(2);
        flushFuture = threadPool.submit(new FlushWorker());
        compactFuture = threadPool.submit(new CompactWorker());
    }

    private static ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> getNewSkipListMap() {
        return new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);
    }

    private static Iterator<Entry<MemorySegment>> getSkipListMapIterator(
            MemorySegment from,
            MemorySegment to,
            ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> dataBase
    ) {
        Iterator<Entry<MemorySegment>> dataBaseIterator;
        if (from == null && to == null) {
            dataBaseIterator = dataBase.values().iterator();
        } else if (from != null && to == null) {
            dataBaseIterator = dataBase.tailMap(from).values().iterator();
        } else if (from == null) {
            dataBaseIterator = dataBase.headMap(to).values().iterator();
        } else {
            dataBaseIterator = dataBase.subMap(from, to).values().iterator();
        }
        return dataBaseIterator;
    }

    private static void addIterator(ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> skipList,
                                    int priority, List<PriorityPeekingIterator<Entry<MemorySegment>>> iterators,
                                    MemorySegment from,
                                    MemorySegment to) {
        if (skipList != null) {
            iterators.add(new PriorityPeekingIterator<>(
                            priority,
                            getSkipListMapIterator(from, to, skipList)
                    )
            );
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (reader.hasNoReaders() && flushQueue.isEmpty() && currentlyFlushing.isEmpty()) {
            PriorityPeekingIterator<Entry<MemorySegment>> peekingIterator
                    = new PriorityPeekingIterator<>(1, getSkipListMapIterator(from, to, memory.get()));
            return new TombstoneRemoverIterator(peekingIterator);
        } else {
            List<PriorityPeekingIterator<Entry<MemorySegment>>> iterators = new LinkedList<>();
            if (!reader.hasNoReaders()) {
                iterators.add(new PriorityPeekingIterator<>(0, reader.get(from, to)));
            }
            addIterator(flushQueue.peek(), 1, iterators, from, to);
            addIterator(currentlyFlushing.peek(), 2, iterators, from, to);
            addIterator(memory.get(), 3, iterators, from, to);
            return new MergeIterator<>(
                    iterators,
                    MemorySegmentComparator.INSTANCE
            );
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = memory.get().get(key);
        if (entry == null) {
            ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> queuePeek = flushQueue.peek();
            if (queuePeek == null) {
                ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> currentPeek = currentlyFlushing.peek();
                if (!(currentPeek == null)) {
                    entry = currentPeek.get(key);
                }
            } else {
                entry = queuePeek.get(key);
            }
        }
        if (entry != null) {
            return entry.value() == null ? null : entry;
        }
        return reader.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        long entrySize = getEntryLength(entry);
        long size = currentByteSize.addAndGet(entrySize);
        if (size >= autoFlushSize) {
            synchronized (memory) {
                if ((size = currentByteSize.get()) >= autoFlushSize) {
                    if (!flushQueue.offer(memory.get())) {
                        throw new BufferOverflowException();
                    }
                    memory.set(getNewSkipListMap());
                    currentByteSize.set(entrySize);
                }
            }
        }
        memory.get().put(entry.key(), entry);
    }

    // informing that before next flush, we need to start compacting
    @Override
    public void compact() throws IOException {
        needToCompact.set(true);
    }

    @Override
    public void flush() throws IOException {
        if (flushQueue.remainingCapacity() == 0) {
            throw new BufferOverflowException();
        }
        ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> newSkipListMap = getNewSkipListMap();
        synchronized (memory) {
            if (!flushQueue.offer(memory.get())) {
                throw new BufferOverflowException();
            }
            memory.set(newSkipListMap);
            currentByteSize.set(0);
        }
    }

    @Override
    public void close() throws IOException {
        if (!isClosed.getAndSet(true)) {
            try {
                // waiting while flush ends
                while (!currentlyFlushing.isEmpty()) {
                    if (currentlyFlushing.isEmpty()) {
                        break;
                    }
                }
                flush();

                flushFuture.get();
                // Waking up thread
                semaphore.release();
                compactFuture.get();

                threadPool.shutdown();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Path getNewFileName() {
        return config.basePath().resolve(newFileId.getAndIncrement() + ".txt");
    }

    private class CompactWorker implements Runnable {
        @Override
        public void run() {
            try {
                semaphore.acquire();
                while (!isClosed.get() || needToCompact.get()) {
                    if (reader.hasNoReaders()) {
                        semaphore.release();
                        return;
                    }
                    Path compactionPath = getNewFileName();
                    semaphore.release();
                    try (FileDBWriter writer = new FileDBWriter(compactionPath)) {
                        if (writer.writeIterable(() -> reader.get(null, null))) {
                            reader.clearAndSet(new FileDBReader(compactionPath));
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Logg
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private class FlushWorker implements Runnable {
        @Override
        public void run() {
            try {
                while (!isClosed.get() || !flushQueue.isEmpty()) {
                    currentlyFlushing.add(flushQueue.take());
                    if (needToCompact.getAndSet(false)) {
                        // Waking up compact thread
                        semaphore.release();
                        // Waiting while compact reserving new file name
                        semaphore.acquire();
                    }
                    ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> current = currentlyFlushing.peek();
                    if (!current.isEmpty()) {
                        Path filePath = getNewFileName();
                        try (FileDBWriter writer = new FileDBWriter(filePath)) {
                            if (writer.writeIterable(current.values())) {
                                reader.add(new FileDBReader(filePath));
                            }
                        }
                    }
                    currentlyFlushing.remove();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
