package ru.mail.polis.levsaskov;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class CompactExecutor implements Runnable {
    public static final boolean POISON_PILL = false;
    private final StorageSystem storageSystem;
    private final BlockingQueue<Boolean> compactionQueue;

    public CompactExecutor(StorageSystem storageSystem, BlockingQueue<Boolean> compactionQueue) {
        this.storageSystem = storageSystem;
        this.compactionQueue = compactionQueue;
    }

    @Override
    public void run() {
        try {
            while (compactionQueue.take()) {
                if (!storageSystem.isCompacted()) {
                    storageSystem.compact();
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Compact executor was interrupted.");
        } catch (IOException e) {
            throw new RuntimeException("IOException during compacting.");
        }
    }
}
