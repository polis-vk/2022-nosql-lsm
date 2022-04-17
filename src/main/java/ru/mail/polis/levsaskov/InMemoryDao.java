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
    private volatile ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> memTable = new ConcurrentSkipListMap<>();
    // Poison pill is empty map
    private final BlockingQueue<ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>>> flushQueue = new ArrayBlockingQueue<>(1);
    // True is signal to start compact, False is poison pill.
    private final BlockingQueue<Boolean> compactionQueue = new LinkedBlockingQueue<>();
    private volatile boolean isClosed;
    private final AtomicLong memTableByteSize = new AtomicLong();
    private long flushThresholdBytes;
    private final StorageSystem storageSystem;
    private Thread compactThread;
    private Thread flushThread;

    public InMemoryDao() {
        storageSystem = null;
    }

    public InMemoryDao(Config config) throws IOException {
        flushThresholdBytes = config.flushThresholdBytes();
        storageSystem = StorageSystem.load(config.basePath());
        compactThread = new Thread(new CompactExecutor(storageSystem, compactionQueue));
        compactThread.start();
        flushThread = new Thread(new FlushExecutor(storageSystem, flushQueue));
        flushThread.start();
    }

    @Override
    public Entry<ByteBuffer> get(ByteBuffer key) throws IOException {
        if (isClosed) {
            throw new RuntimeException("In memory dao closed.");
        }

        Entry<ByteBuffer> ans = memTable.get(key);

        if (ans == null) {
            var flushingTable = flushQueue.peek();
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
        if (isClosed) {
            throw new RuntimeException("In memory dao closed.");
        }

        var cutMemTable = cutMemTable(memTable, from, to);
        return storageSystem == null ? cutMemTable.values().iterator() :
                storageSystem.getMergedEntrys(from, to, cutMemTable, cutMemTable(flushQueue.peek(), from, to));
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        if (isClosed) {
            throw new RuntimeException("In memory dao closed.");
        }

        if (memTableByteSize.addAndGet(entry.key().capacity() + entry.value().capacity()) > flushThresholdBytes) {
            // Add() throws exception if queue is full, so upsert will not be done.
            flushQueue.add(memTable);
            // Create new in memory memTable if queue of flushing not full (exception wasn't thrown)
            memTable = new ConcurrentSkipListMap<>();
            memTableByteSize.set(entry.key().capacity() + entry.value().capacity());
        }

        memTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (isClosed) {
            throw new RuntimeException("In memory dao closed.");
        }

        if (!memTable.isEmpty()) {
            // Empty mem table is poison bill.
            // Throws exception if flush queue is full
            flushQueue.add(memTable);
        }
    }

    @Override
    public void compact() throws IOException {
        if (isClosed) {
            throw new RuntimeException("In memory dao closed.");
        }

        compactionQueue.add(true);
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }

        flush();
        try {
            compactionQueue.put(CompactExecutor.POISON_PILL);
            flushQueue.put(FlushExecutor.POISON_PILL);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted Queue");
        }

        isClosed = true;
        try {
            compactThread.join();
            flushThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException("Join while closing was interrupted.");
        }
        storageSystem.close();
    }

    /**
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
