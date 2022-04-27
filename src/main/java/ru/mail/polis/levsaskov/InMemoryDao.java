package ru.mail.polis.levsaskov;

import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryDao implements Dao<ByteBuffer, Entry<ByteBuffer>> {
    private final long flushThresholdBytes;
    private final StorageSystem storageSystem;
    private volatile ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> memTable = new ConcurrentSkipListMap<>();
    private final AtomicLong memTableByteSize = new AtomicLong();
    // Poison pill is empty map
    private final BlockingQueue<ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>>> flushQueue =
            new ArrayBlockingQueue<>(1);
    // True is signal to start compact, False is poison pill.
    private final BlockingQueue<Boolean> compactionQueue = new LinkedBlockingQueue<>();
    private final Thread compactThread;
    private final Thread flushThread;
    private final FlushExecutor flushExecutor;
    private volatile boolean isClosed;

    public InMemoryDao() {
        storageSystem = null;
        flushThread = null;
        flushExecutor = null;
        compactThread = null;
        flushThresholdBytes = 0;
    }

    public InMemoryDao(Config config) throws IOException {
        flushThresholdBytes = config.flushThresholdBytes();
        storageSystem = StorageSystem.load(config.basePath());
        compactThread = new Thread(new CompactExecutor(storageSystem, compactionQueue));
        compactThread.start();
        flushExecutor = new FlushExecutor(storageSystem, flushQueue);
        flushThread = new Thread(flushExecutor);
        flushThread.start();
    }

    @Override
    public Entry<ByteBuffer> get(ByteBuffer key) throws IOException {
        checkClose();

        Entry<ByteBuffer> ans = memTable.get(key);

        if (ans == null) {
            var flushingTable = flushQueue.peek();
            if (flushingTable == null) {
                flushingTable = flushExecutor.getInFlushing(); // Try to avoid race
            }

            ans = flushingTable == null ? null : flushingTable.get(key);
        }
        if (ans == null && storageSystem != null) {
            ans = storageSystem.findEntry(key);
        }
        if (ans == null || ans.value() == null) {
            return null;
        }

        return ans;
    }

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        checkClose();

        var cutMemTable = cutMemTable(memTable, from, to);
        if (storageSystem == null && cutMemTable != null) {
            return cutMemTable.values().iterator();
        }

        return storageSystem == null ? null : storageSystem.getMergedEntrys(from, to, cutMemTable,
                cutMemTable(flushQueue.peek(), from, to), // Try to avoid race
                cutMemTable(flushExecutor.getInFlushing(), from, to));
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        checkClose();

        int entrySize = StoragePart.getPersEntryByteSize(entry);
        if (memTableByteSize.addAndGet(entrySize) > flushThresholdBytes) {
            // Add() throws exception if queue is full, so upsert will not be done.
            flushQueue.add(memTable);
            // Create new in memory memTable if queue of flushing not full (exception wasn't thrown)
            memTable = new ConcurrentSkipListMap<>();
            memTableByteSize.set(entrySize);
        }

        memTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        checkClose();

        if (!memTable.isEmpty()) {
            // Empty mem table is poison bill.
            // Throws exception if flush queue is full
            flushQueue.add(memTable);
            memTable = new ConcurrentSkipListMap<>();
            memTableByteSize.set(0);
        }
    }

    @Override
    public void compact() throws IOException {
        checkClose();
        compactionQueue.add(true);
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }

        isClosed = true;
        try {
            flushQueue.put(memTable);
            compactionQueue.put(CompactExecutor.POISON_PILL);
            flushQueue.put(FlushExecutor.POISON_PILL);
            compactThread.join();
            flushThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        storageSystem.close();
    }

    private void checkClose() {
        if (isClosed) {
            throw new RuntimeException("In memory dao closed.");
        }
    }

    /**
     * Cuts mem table in given range.
     *
     * @return null if memTable is null, otherwise cut with given range memTable.
     */
    private static ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> cutMemTable(
            ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> memTable,
            ByteBuffer from, ByteBuffer to) {
        ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> cut;

        if ((from == null && to == null) || memTable == null) {
            cut = memTable;
        } else if (from == null) {
            cut = memTable.headMap(to);
        } else if (to == null) {
            cut = memTable.tailMap(from);
        } else {
            cut = memTable.subMap(from, to);
        }

        return cut;
    }
}
