package ru.mail.polis.levsaskov;

import ru.mail.polis.Entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class FlushExecutor implements Runnable {
    // Poison pill is empty map
    public static final ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> POISON_PILL = new ConcurrentSkipListMap<>();
    private final BlockingQueue<ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>>> flushQueue;
    private final StorageSystem storageSystem;

    public FlushExecutor(StorageSystem storageSystem, BlockingQueue<ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>>> flushQueue) {
        this.storageSystem = storageSystem;
        this.flushQueue = flushQueue;
    }

    @Override
    public void run() {
        ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> msg;
        try {
            // take is blocking
            while (!isPoisonPill(msg = flushQueue.take())) {
                storageSystem.save(msg);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Flush executor was interrupted.");
        } catch (IOException e) {
            throw new RuntimeException("IOException during flushing.");
        }
    }

    private static boolean isPoisonPill(ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> memTable) {
        return memTable.isEmpty();
    }
}
